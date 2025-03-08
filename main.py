import requests
import json
import time
import pandas as pd
import os
import argparse
import logging
from datetime import datetime, timedelta
from tqdm import tqdm
import calendar
import warnings
import threading
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

# Настройка логирования
try:
    os.makedirs('logs', exist_ok=True)
    log_file = f'logs/parser_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        encoding='utf-8'
    )
    
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    logger = logging.getLogger('belgiss_parser')
    logger.addHandler(file_handler)
    
    logger.info("Логирование инициализировано")
except Exception as e:
    print(f"Ошибка при инициализации логирования: {e}")
    # Создаем заглушку для логгера, чтобы код работал даже при ошибке логирования
    class DummyLogger:
        def info(self, msg): pass
        def error(self, msg): print(f"ОШИБКА: {msg}")
        def warning(self, msg): pass
        def debug(self, msg): pass
    logger = DummyLogger()

# Отключаем предупреждения о небезопасных запросах
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

class BelgissParser:
    def __init__(self, max_retries=10, retry_delay=2, verify_ssl=False, max_threads=40, delay_range=(0.1, 1.0)):
        self.base_url = "https://tsouz.belgiss.by"  # Основной URL сайта
        self.site_url = "https://tsouz.belgiss.by"
        self.api_url = "https://api.belgiss.by/tsouz"  # API URL - здесь правильный адрес API
        self.verify_ssl = verify_ssl
        self.max_threads = max_threads
        # Начинаем с меньшего числа потоков (консервативный подход)
        self.current_threads = min(6, max(4, self.max_threads // 8))
        self.max_retries = max_retries
        self.current_retries = max_retries
        self.retry_delay = retry_delay
        self.delay_range = delay_range
        self.error_counter = 0
        self.success_counter = 0
        self.error_threshold = 3
        self.success_threshold = 10  # Увеличиваем порог для более консервативного роста потоков
        
        self.consecutive_fails = 0
        self.consecutive_success = 0
        self.adaptive_delay = delay_range[0]
        
        # Кэши для минимизации повторных запросов
        self._page_cache = {}
        self._details_cache = {}
        self._failed_urls = set()  # Множество URL, на которых были ошибки
        
        # Добавляем семафор для контроля одновременных запросов
        self.request_semaphore = threading.Semaphore(10)  # Максимум 10 одновременных запросов
        
        # Добавляем статистику запросов
        self.request_stats = {
            'success': 0,
            'failed': 0,
            'cached': 0,
            'total_time': 0
        }
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 YaBrowser/25.2.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "ru",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Connection": "keep-alive",
            "Origin": "https://tsouz.belgiss.by",
            "Referer": "https://tsouz.belgiss.by/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "X-Requested-With": "XMLHttpRequest"
        }
        
        self.lock = threading.Lock()
        self.output_folder = os.path.abspath('.')
        
        # Инициализируем сессию с настроенной стратегией повторов для HTTP
        self.session = requests.Session()
        
        # Настраиваем более агрессивную стратегию соединений
        adapter = HTTPAdapter(
            max_retries=Retry(
                total=max_retries,
                backoff_factor=0.3,
                status_forcelist=[500, 502, 503, 504, 429],
                allowed_methods=["GET", "POST"]
            ),
            pool_connections=max(50, max_threads),  # Увеличиваем пул соединений
            pool_maxsize=max(50, max_threads)  # Увеличиваем максимальный размер пула
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        # Загружаем кэш, если он существует
        self.load_cache()
        
        logger.info("Инициализация парсера успешно завершена")
    
    def set_cookies(self, cookies_dict):
        """Устанавливает дополнительные куки в сессию"""
        if not cookies_dict:
            return
            
        print("Установка дополнительных куков в сессию...")
        for name, value in cookies_dict.items():
            self.session.cookies.set(name, value, domain=".belgiss.by", path="/")
            
        print(f"Установлены куки: {list(cookies_dict.keys())}")
        print(f"Текущие куки сессии: {dict(self.session.cookies)}")
    
    def make_request(self, method, url, **kwargs):
        # Используем семафор для ограничения одновременных запросов
        with self.request_semaphore:
            if "headers" not in kwargs:
                kwargs["headers"] = self.headers
            
            kwargs["verify"] = self.verify_ssl
            
            timeout = kwargs.pop('timeout', 30)
            
            # Вычисляем соотношение успешных запросов
            total_requests = self.request_stats['success'] + self.request_stats['failed']
            success_ratio = self.request_stats['success'] / max(1, total_requests)
                
            start_time = time.time()
            
            for attempt in range(self.current_retries):
                try:
                    # Адаптивная задержка зависит от успешности прошлых запросов
                    delay = random.uniform(0.05, 0.2) if attempt == 0 and success_ratio > 0.8 else random.uniform(0.1, self.delay_range[1])
                    time.sleep(delay)
                    
                    response = self.session.request(method, url, timeout=timeout, **kwargs)
                    
                    status_code = response.status_code
                    if status_code == 429:
                        wait_time = min(self.retry_delay * (1.2 ** attempt) + random.uniform(0.5, 1), 8)
                        logger.info(f"Лимит запросов (429), ожидание {wait_time:.1f} сек...")
                        time.sleep(wait_time)
                        self.adjust_threads_and_retries(success=False)
                        continue
                        
                    response.raise_for_status()
                    
                    if response.text.strip():
                        try:
                            result = response.json()
                            self.adjust_threads_and_retries(success=True)
                            self.request_stats['success'] += 1
                            self.request_stats['total_time'] += time.time() - start_time
                            return result
                        except json.JSONDecodeError:
                            logger.error(f"Ошибка декодирования JSON из ответа для {url}")
                            self.adjust_threads_and_retries(success=False)
                            self.request_stats['failed'] += 1
                            if attempt < self.current_retries - 1:
                                sleep_time = min(self.retry_delay * (attempt + 0.3) + random.uniform(0.3, 0.8), 5)
                                time.sleep(sleep_time)
                            else:
                                self.request_stats['total_time'] += time.time() - start_time
                                return None
                    else:
                        logger.error("Получен пустой ответ от сервера")
                        self.adjust_threads_and_retries(success=False)
                        self.request_stats['failed'] += 1
                        if attempt < self.current_retries - 1:
                            sleep_time = min(self.retry_delay * (attempt + 0.3) + random.uniform(0.3, 0.8), 5)
                            time.sleep(sleep_time)
                        else:
                            self.request_stats['total_time'] += time.time() - start_time
                            return None
                            
                except requests.exceptions.HTTPError as e:
                    logger.error(f"HTTP ошибка: {e}")
                    self.adjust_threads_and_retries(success=False)
                    sleep_time = min(self.retry_delay * (attempt + 0.3) + random.uniform(0.3, 0.8), 5)
                    
                    if attempt < self.current_retries - 1:
                        logger.info(f"Повторная попытка через {sleep_time:.1f} сек...")
                        time.sleep(sleep_time)
                    else:
                        self.request_stats['failed'] += 1
                        self.request_stats['total_time'] += time.time() - start_time
                        return None
                        
                except requests.exceptions.ReadTimeout:
                    logger.error(f"Таймаут чтения для {url}")
                    self.adjust_threads_and_retries(success=False)
                    if attempt < self.current_retries - 1:
                        sleep_time = min(self.retry_delay * (attempt + 0.5) + random.uniform(0.5, 1), 6)
                        logger.info(f"Повторная попытка через {sleep_time:.1f} сек...")
                        time.sleep(sleep_time)
                    else:
                        self.request_stats['failed'] += 1
                        self.request_stats['total_time'] += time.time() - start_time
                        return None
                        
                except requests.exceptions.ConnectionError:
                    logger.error(f"Ошибка соединения для {url}")
                    self.adjust_threads_and_retries(success=False)
                    if attempt < self.current_retries - 1:
                        sleep_time = min(self.retry_delay * (attempt + 0.5) + random.uniform(0.5, 1), 6)
                        logger.info(f"Повторная попытка через {sleep_time:.1f} сек...")
                        time.sleep(sleep_time)
                    else:
                        self.request_stats['failed'] += 1
                        self.request_stats['total_time'] += time.time() - start_time
                        return None
                        
                except Exception as e:
                    logger.error(f"Непредвиденная ошибка при запросе {url}: {e}")
                    self.adjust_threads_and_retries(success=False)
                    if attempt < self.current_retries - 1:
                        sleep_time = min(self.retry_delay * (attempt + 0.3) + random.uniform(0.3, 0.7), 5)
                        time.sleep(sleep_time)
                    else:
                        self.request_stats['failed'] += 1
                        self.request_stats['total_time'] += time.time() - start_time
                        return None
                        
            self.request_stats['failed'] += 1
            self.request_stats['total_time'] += time.time() - start_time
            return None
    
    def format_date(self, date_obj):
        """Форматирование даты для запроса"""
        return date_obj.strftime("%d.%m.%Y")
    
    def get_certifications_by_date_range(self, start_date, end_date, page=1, per_page=100):
        """Получает информацию о сертификатах/декларациях за указанный диапазон дат"""
        if isinstance(start_date, datetime):
            start_date_str = self.format_date(start_date)
        else:
            start_date_str = start_date
            
        if isinstance(end_date, datetime):
            end_date_str = self.format_date(end_date)
        else:
            end_date_str = end_date
        
        # Используем правильный URL для API в соответствии с HAR-файлом
        url = f"{self.api_url}/tsouz-certifs-light"
        
        # Параметры запроса в соответствии с текущим форматом сайта
        params = {
            "page": page,
            "per-page": per_page,
            "sort": "-certdecltr_id",  # Сортировка по убыванию ID
            "filter[DocStartDate][gte]": start_date_str,  # Начальная дата
            "filter[DocStartDate][lte]": end_date_str,    # Конечная дата
            "query[trts]": 1  # Параметр для поиска сертификатов и деклараций
        }
        
        # Кэш для повторных запросов - предотвращаем дублирование запросов к одной странице
        cache_key = f"{start_date_str}_{end_date_str}_{page}_{per_page}"
        if hasattr(self, '_page_cache') and cache_key in self._page_cache:
            return self._page_cache[cache_key]
        
        # Делаем запрос через сессию для сохранения куков
        response = self.make_request("GET", url, params=params)
        
        # Обработка ответа с учетом формата текущего API
        if response and isinstance(response, dict):
            # Адаптируем ответ к нашему формату
            items = response.get('items', [])
            meta = response.get('_meta', {})
            result = {
                'items': items,
                'pages': meta.get('pageCount', 0),
                'count': meta.get('totalCount', 0),
                'current_page': meta.get('currentPage', page)
            }
            
            # Сохранить в кэш, если запрос успешен
            if not hasattr(self, '_page_cache'):
                self._page_cache = {}
            self._page_cache[cache_key] = result
            
            return result
        
        return None
    
    def get_certification_details(self, doc_id):
        """Получает детальную информацию о сертификате/декларации по его ID"""
        if not doc_id:
            logger.warning("Передан пустой ID сертификата")
            return None
            
        # Проверяем, есть ли данные в кэше
        if doc_id in self._details_cache:
            self.request_stats['cached'] += 1
            return self._details_cache[doc_id]
            
        # Используем правильный URL для получения деталей сертификата
        url = f"{self.api_url}/tsouz-certifs/{doc_id}"
        
        # Делаем запрос через сессию для сохранения куков
        response = self.make_request("GET", url)
        
        # Сохраняем результат в кэш, если запрос успешен
        if response:
            self._details_cache[doc_id] = response
            
        return response
    
    def get_certification_details_batch(self, doc_ids, max_ids_per_request=5):
        """Получает детальную информацию для нескольких сертификатов за один запрос
        
        Примечание: Этот метод следует использовать только если API поддерживает 
        пакетные запросы. В противном случае он будет выполнять запросы последовательно.
        """
        results = {}
        
        # Проверяем кэш для каждого ID
        ids_to_fetch = [doc_id for doc_id in doc_ids if doc_id not in self._details_cache]
        
        # Если все ID уже в кэше, возвращаем кэшированные данные
        if not ids_to_fetch:
            self.request_stats['cached'] += len(doc_ids)
            return {doc_id: self._details_cache[doc_id] for doc_id in doc_ids}
        
        # Проверяем, поддерживает ли API пакетные запросы
        # Если API поддерживает пакетные запросы, используем их
        try:
            # Разбиваем запрашиваемые ID на группы по max_ids_per_request
            id_groups = [ids_to_fetch[i:i+max_ids_per_request] for i in range(0, len(ids_to_fetch), max_ids_per_request)]
            
            for group in id_groups:
                # Формируем строку с ID через запятую или другой разделитель согласно API
                ids_param = ','.join(group)
                url = f"{self.api_url}/tsouz-certifs-batch"  # Предположительный URL для пакетного запроса
                params = {"ids": ids_param}
                
                with self.request_semaphore:
                    response = self.make_request("GET", url, params=params)
                
                if response and isinstance(response, list):
                    # Предполагаем, что ответ - список объектов с деталями
                    for item in response:
                        doc_id = item.get('DocId', '')
                        if doc_id:
                            self._details_cache[doc_id] = item
                            results[doc_id] = item
                else:
                    # Если пакетный запрос не поддерживается или вернул ошибку,
                    # выполняем запросы последовательно для каждого ID
                    logger.warning("Пакетный запрос не поддерживается или вернул ошибку. Выполняем запросы последовательно.")
                    for doc_id in group:
                        result = self.get_certification_details(doc_id)
                        if result:
                            results[doc_id] = result
        except Exception as e:
            logger.error(f"Ошибка при выполнении пакетного запроса: {e}")
            # В случае ошибки выполняем запросы последовательно
            for doc_id in ids_to_fetch:
                result = self.get_certification_details(doc_id)
                if result:
                    results[doc_id] = result
        
        # Добавляем кэшированные результаты
        for doc_id in doc_ids:
            if doc_id in self._details_cache and doc_id not in results:
                results[doc_id] = self._details_cache[doc_id]
                self.request_stats['cached'] += 1
        
        return results
    
    def fetch_page_worker(self, page, start_date, end_date, per_page, result_dict, pbar):
        """Рабочая функция для получения страницы результатов в отдельном потоке"""
        page_data = self.get_certifications_by_date_range(start_date, end_date, page=page, per_page=per_page)
        if page_data and page_data.get('items'):
            with self.lock:
                # Получаем ID документов из новой структуры ответа
                result_dict[page] = [item.get('certdecltr_id') for item in page_data.get('items', [])]
                # Обновляем описание прогресс-бара в едином формате
                percent = int((pbar.n + 1) / pbar.total * 100)
                pbar.set_description(f"[1/2] Сбор списка | Потоки: {self.current_threads} | Стр: {page}")
                logger.info(f"Получено {len(result_dict[page])} сертификатов со страницы {page}")
        else:
            with self.lock:
                percent = int((pbar.n + 1) / pbar.total * 100)
                pbar.set_description(f"[1/2] Сбор списка | Потоки: {self.current_threads} | Стр: {page} (пусто)")
                logger.warning(f"Не удалось получить данные для страницы {page}")
        
        pbar.update(1)
        # Оптимизированная адаптивная задержка
        time.sleep(max(0.01, self.adaptive_delay / 4))  
    
    def fetch_cert_details_worker(self, cert_id, result_list, pbar):
        cert_details = self.get_certification_details(cert_id)
        if cert_details:
            try:
                processed_data = self.process_cert_data(cert_details)
                if processed_data:
                    with self.lock:
                        # Добавляем только если данные были успешно обработаны
                        if processed_data not in result_list:
                            result_list.append(processed_data)
                        pbar.set_description(f"[2/2] Детали | Потоки: {self.current_threads} | ID: {cert_id}")
                        pbar.update(1)
                else:
                    with self.lock:
                        logger.warning(f"Не удалось обработать данные для сертификата {cert_id}")
                        pbar.set_description(f"[2/2] Ошибка обработки | ID: {cert_id}")
                        pbar.update(1)
            except Exception as e:
                with self.lock:
                    logger.error(f"Ошибка при обработке сертификата {cert_id}: {e}")
                    pbar.set_description(f"[2/2] Ошибка | ID: {cert_id}")
                    pbar.update(1)
        else:
            with self.lock:
                logger.warning(f"Не удалось получить детали для сертификата {cert_id}")
                pbar.set_description(f"[2/2] Нет данных | ID: {cert_id}")
                pbar.update(1)
                
        # Оптимизированная адаптивная задержка
        time.sleep(max(0.01, self.adaptive_delay / 4))
    
    def parse_data_for_date_range(self, start_date, end_date, max_certs=None, per_page=100):
        """Получает и обрабатывает данные о сертификатах за указанный период"""
        # Форматируем даты и получаем данные за указанный период
        start_date_str = self.format_date(start_date)
        end_date_str = self.format_date(end_date)
        
        # Получаем первую страницу, чтобы определить общее количество страниц
        data = self.get_certifications_by_date_range(start_date, end_date, page=1, per_page=per_page)
        
        if not data or not data.get('items'):
            logger.error(f"Не удалось получить данные за период {start_date_str} - {end_date_str}")
            return []
        
        # Получаем общее количество элементов и страниц
        total_count = data.get('count', 0)
        total_pages = data.get('pages', 0)
        
        if total_count == 0 or total_pages == 0:
            logger.warning(f"За период {start_date_str} - {end_date_str} нет данных")
            return []
            
        logger.info(f"Всего найдено {total_count} записей на {total_pages} страницах")
        
        # Если задано ограничение на количество сертификатов
        if max_certs and max_certs < total_count:
            total_count = max_certs
            # Пересчитываем количество страниц с учетом ограничения
            total_pages = min(total_pages, (max_certs + per_page - 1) // per_page)
            logger.info(f"Ограничение: будет обработано {max_certs} записей на {total_pages} страницах")
        
        # Подготавливаем словарь для результатов первого этапа
        # Данные первой страницы уже получены, добавляем их сразу
        all_cert_ids = {}
        all_cert_ids[1] = [item.get('certdecltr_id') for item in data.get('items', [])]
        
        # Устанавливаем 2 потока для параллельной обработки
        self.current_threads = min(8, max(4, self.max_threads // 5))
        
        if total_pages > 1:
            # Подготавливаем словарь для результатов
            result_dict = {}  # Словарь для хранения результатов
            
            # Используем tqdm для отображения прогресса
            with tqdm(total=total_pages - 1, desc=f"[1/2] Сбор списка | Потоки: {self.current_threads}", 
                       bar_format='{desc} | {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]',
                       position=0, leave=True, colour='green') as pbar:
                
                # Группируем страницы по 2
                pages = list(range(2, total_pages + 1))
                batch_size = self.current_threads
                page_batches = [pages[i:i+batch_size] for i in range(0, len(pages), batch_size)]
                
                for batch in page_batches:
                    # Обрабатываем страницы в текущем пакете
                    with ThreadPoolExecutor(max_workers=self.current_threads) as executor:
                        futures = []
                        for page in batch:
                            future = executor.submit(
                                self.fetch_page_worker, 
                                page, 
                                start_date, 
                                end_date, 
                                per_page, 
                                result_dict, 
                                pbar
                            )
                            futures.append(future)
                            
                        # Ждем завершения всех задач в текущем пакете
                        for future in as_completed(futures):
                            try:
                                future.result()
                            except Exception as e:
                                logger.error(f"Ошибка при получении страницы: {e}")
                    
                    # Фиксированная пауза 200 мс между батчами
                    if batch != page_batches[-1]:
                        pbar.set_description(f"[1/2] Пауза 200мс | След. стр: {page_batches[page_batches.index(batch)+1][:3]}")
                        time.sleep(0.2)
            
            # Объединяем результаты
            for page, ids in result_dict.items():
                all_cert_ids[page] = ids
        
        # Объединяем все ID сертификатов в один список
        cert_ids = []
        for page in sorted(all_cert_ids.keys()):
            cert_ids.extend(all_cert_ids[page])
            
        # Если задано ограничение, обрезаем список ID
        if max_certs and len(cert_ids) > max_certs:
            cert_ids = cert_ids[:max_certs]
            
        total_certs = len(cert_ids)
        logger.info(f"Получено {total_certs} ID сертификатов/деклараций")
        
        if total_certs == 0:
            logger.warning("Нет данных для обработки")
            return []
        
        # Получаем детальную информацию о каждом сертификате по 2 за раз
        result_list = []
        
        # Используем tqdm для отображения прогресса
        with tqdm(total=total_certs, desc=f"[2/2] Детали | Потоки: {self.current_threads}", 
                  bar_format='{desc} | {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]',
                  position=0, leave=True, colour='blue') as pbar:
            
            # Увеличиваем размер батча для более эффективной обработки
            batch_size = self.current_threads * 2  # Увеличиваем размер батча в 2 раза
            id_batches = [cert_ids[i:i+batch_size] for i in range(0, len(cert_ids), batch_size)]
            
            for batch in id_batches:
                # Обрабатываем ID в текущем пакете
                with ThreadPoolExecutor(max_workers=self.current_threads) as executor:
                    futures = []
                    for cert_id in batch:
                        future = executor.submit(
                            self.fetch_cert_details_worker, 
                            cert_id, 
                            result_list, 
                            pbar
                        )
                        futures.append(future)
                        
                    # Ждем завершения всех задач в текущем пакете
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            logger.error(f"Ошибка при получении деталей: {e}")
                
                # Фиксированная пауза 300 мс между батчами
                if batch != id_batches[-1]:
                    next_batch = id_batches[id_batches.index(batch)+1][:3]
                    # Адаптивная задержка между батчами вместо фиксированной 300 мс
                    adaptive_batch_delay = max(0.1, min(0.5, self.adaptive_delay * 1.5 + random.uniform(0.05, 0.2)))
                    pbar.set_description(f"[2/2] Адапт. пауза {adaptive_batch_delay:.2f}с | След. ID: {next_batch}")
                    time.sleep(adaptive_batch_delay)
        
        logger.info(f"Получено {len(result_list)} записей с детальной информацией")
        return result_list
    
    def process_cert_data(self, cert_data):
        result = {}
        
        if not cert_data or not isinstance(cert_data, dict):
            logger.error(f"Получены некорректные данные для обработки")
            result['Регистрационный номер'] = "Ошибка получения данных"
            return result
        
        cert_details = cert_data.get('certdecltr_ConformityDocDetails', {})
        
        doc_id = cert_data.get('DocId', '') or cert_details.get('DocId', '')
        result['Регистрационный номер'] = doc_id
        
        result['Дата начала действия'] = cert_details.get('DocStartDate', '')
        result['Дата окончания действия'] = cert_details.get('DocValidityDate', '')
        
        doc_status = cert_details.get('DocStatusDetails', {})
        status_code = doc_status.get('DocStatusCode', '')
        
        status_mapping = {
            '01': 'Действует',
            '02': 'Прекращен',
            '03': 'Приостановлен',
            '04': 'Возобновлен',
            '05': 'Архивный',
        }
        result['Статус действия'] = status_mapping.get(status_code, f'Неизвестный ({status_code})')
        
        doc_kind_code = cert_data.get('ConformityDocKindCode', '')
        kind_mapping = {
            '01': 'Сертификат соответствия на продукцию',
            '02': 'Сертификат соответствия на услуги',
            '03': 'Сертификат соответствия на систему менеджмента',
            '05': 'Декларация о соответствии',
            '10': 'Сертификат соответствия ТР ТС/ЕАЭС',
            '15': 'Декларация о соответствии ТР ТС/ЕАЭС',
        }
        result['Вид документа'] = kind_mapping.get(doc_kind_code, f'Неизвестный ({doc_kind_code})')
        
        tech_regs = cert_details.get('TechnicalRegulationId', [])
        result['Номер технического регламента'] = ', '.join(tech_regs) if tech_regs else ''
        
        conformity_authority = cert_details.get('ConformityAuthorityV2Details', {})
        result['Орган по сертификации'] = conformity_authority.get('BusinessEntityName', '')
        result['Код органа'] = conformity_authority.get('ConformityAuthorityId', '')
        
        applicant = cert_details.get('ApplicantDetails', {})
        result['Заявитель - Наименование'] = applicant.get('BusinessEntityName', '')
        result['Заявитель - Страна'] = applicant.get('UnifiedCountryCode', '')
        
        manufacturer = cert_details.get('ManufacturerDetails', [{}])[0] if cert_details.get('ManufacturerDetails') else {}
        result['Производитель - Наименование'] = manufacturer.get('BusinessEntityName', '')
        result['Производитель - Страна'] = manufacturer.get('UnifiedCountryCode', '')
        
        product_details = cert_details.get('ProductDetails', [])
        if product_details:
            product = product_details[0]
            result['Продукция - Наименование'] = product.get('ProductName', '')
            result['Продукция - Тип'] = product.get('ProductTypeCodeName', '')
            result['Продукция - Код ТНВЭД'] = ', '.join(product.get('CommodityCodeList', []))
        
        return result
    
    def save_to_excel(self, data, filename=None):
        if not data:
            logger.error("Нет данных для сохранения")
            return None
        
        # Проверка полноты данных
        has_only_reg_number = all(len(item.keys()) <= 1 and 'Регистрационный номер' in item for item in data)
        if has_only_reg_number:
            logger.warning("ВНИМАНИЕ: Данные содержат только регистрационные номера без дополнительной информации!")
            
            empty_fields = True
            for item in data:
                if any(val for val in item.values() if val):
                    empty_fields = False
                    break
                    
            if empty_fields:
                logger.error("Все поля пусты! Возможно проблемы с получением данных.")
        
        os.makedirs(self.output_folder, exist_ok=True)
            
        if not filename:
            now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            filename = f"certifications_{now}.xlsx"
        
        filepath = os.path.join(self.output_folder, filename)
        
        try:
            num_rows = len(data)
            num_cols = len(data[0].keys()) if data else 0
            logger.info(f"Сохранение {num_rows} строк с {num_cols} колонками в {filepath}")
            
            df = pd.DataFrame(data)
            df.to_excel(filepath, index=False, engine='openpyxl')
            
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                print(f"\nФайл сохранен: {filepath}")
                print(f"Строк: {num_rows}, Колонок: {num_cols}, Размер: {os.path.getsize(filepath)/1024:.1f} KB")
                logger.info(f"Файл сохранен успешно: {filepath} ({os.path.getsize(filepath)/1024:.1f} KB)")
                return filepath
            else:
                logger.error(f"Ошибка: файл {filepath} не создан или пуст")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка при сохранении данных в Excel: {e}")
            return None
    
    def parse_data_for_month(self, year, month, max_certs=None):
        """Парсинг данных за указанный месяц"""
        start_date = datetime(year, month, 1)
        
        # Определяем последний день месяца
        last_day = calendar.monthrange(year, month)[1]
        end_date = datetime(year, month, last_day)
        
        return self.parse_data_for_date_range(start_date, end_date, max_certs=max_certs)
    
    def adjust_threads_and_retries(self, success=True):
        """Автоматически регулирует число потоков и повторов в зависимости от успешности запросов"""
        with self.lock:
            if success:
                self.success_counter += 1
                self.error_counter = max(0, self.error_counter - 1)
                self.consecutive_fails = 0
                self.consecutive_success += 1
                
                # Адаптивно уменьшаем задержку при успешных запросах
                if self.consecutive_success > 5:
                    self.adaptive_delay = max(0.01, self.adaptive_delay * 0.9)
                
                # Более консервативное увеличение потоков (только на 1 за раз)
                if self.consecutive_success >= 10 and self.current_threads < self.max_threads:
                    increment = 1  # Увеличиваем только на 1 вместо 2 или 4
                    self.current_threads = min(self.max_threads, self.current_threads + increment)
                    self.success_counter = 0
                    logger.info(f"Увеличено число потоков до {self.current_threads}")
            else:
                self.error_counter += 1
                self.success_counter = max(0, self.success_counter - 1)
                self.consecutive_success = 0
                self.consecutive_fails += 1
                
                # Адаптивно увеличиваем задержку при ошибках
                if self.consecutive_fails > 2:
                    self.adaptive_delay = min(self.delay_range[1] * 1.2, self.adaptive_delay * 1.1)
                
                # Быстрее снижаем число потоков при ошибках
                if self.error_counter >= self.error_threshold:
                    if self.current_threads > 2:
                        decrement = 2 if self.consecutive_fails >= 2 else 1  # Быстрее уменьшаем потоки
                        self.current_threads = max(2, self.current_threads - decrement)
                        logger.info(f"Уменьшено число потоков до {self.current_threads}")
                    
                    # Увеличиваем число повторов при частых ошибках, но ограничиваем максимум
                    self.current_retries = min(self.max_retries + 2, self.current_retries + 1)
                    self.error_counter = 0
                    logger.info(f"Увеличено число повторов до {self.current_retries}")

    # Добавляем методы для сохранения и загрузки кэша
    def save_cache(self, filename='cache_details.json'):
        try:
            # Сохраняем только те поля, которые можно сериализовать в JSON
            cache_to_save = {}
            for k, v in self._details_cache.items():
                try:
                    json.dumps(v)  # Проверяем, что объект можно сериализовать
                    cache_to_save[k] = v
                except:
                    pass  # Пропускаем объекты, которые нельзя сериализовать
                    
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(cache_to_save, f)
            logger.info(f"Кэш сохранен, {len(cache_to_save)} записей")
            return len(cache_to_save)
        except Exception as e:
            logger.error(f"Ошибка при сохранении кэша: {e}")
            return 0
    
    def load_cache(self, filename='cache_details.json'):
        if os.path.exists(filename):
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    loaded_cache = json.load(f)
                    self._details_cache.update(loaded_cache)
                logger.info(f"Кэш загружен, {len(loaded_cache)} записей")
                return len(loaded_cache)
            except Exception as e:
                logger.error(f"Ошибка при загрузке кэша: {e}")
                return 0
        return 0

def interactive_date_input():
    """Интерактивный ввод дат в консоли"""
    try:
        today = datetime.today()
        
        while True:
            # Запрашиваем диапазон дат
            print("\nВведите даты в формате ДД.ММ.ГГГГ")
            start_date_str = input("Начальная дата: ")
            end_date_str = input("Конечная дата: ")
            
            try:
                start_date = datetime.strptime(start_date_str, "%d.%m.%Y")
                end_date = datetime.strptime(end_date_str, "%d.%m.%Y")
                
                if end_date < start_date:
                    print("Ошибка: дата окончания не может быть раньше даты начала")
                    return None, None
                
                return start_date, end_date
            
            except ValueError as e:
                print(f"Ошибка в формате даты: {e}")
                print("Убедитесь, что даты указаны в формате ДД.ММ.ГГГГ")
    except Exception as e:
        print(f"Ошибка при вводе дат: {e}")
        return None, None

def main():
    parser = argparse.ArgumentParser(description="Парсер сертификатов/деклараций БелГИСС")
    
    parser.add_argument('-d', '--date-range', nargs=2, metavar=('START_DATE', 'END_DATE'),
                        help='Диапазон дат для парсинга в формате DD.MM.YYYY')
    parser.add_argument('-m', '--month', type=int, 
                        help='Месяц для парсинга (1-12)')
    parser.add_argument('-y', '--year', type=int, default=datetime.now().year,
                        help='Год для парсинга (по умолчанию - текущий год)')
    parser.add_argument('-o', '--output', help='Имя выходного файла')
    parser.add_argument('-n', '--max-certs', type=int, 
                        help='Максимальное количество сертификатов для обработки')
    
    parser.add_argument('--retries', type=int, default=10,
                        help='Количество попыток при ошибках запросов (по умолчанию 10)')
    parser.add_argument('--delay', type=float, default=2.0,
                        help='Базовая задержка между попытками в секундах (по умолчанию 2.0)')
    parser.add_argument('--threads', type=int, default=40,
                        help='Максимальное количество потоков (по умолчанию 40)')
    parser.add_argument('--delay-min', type=float, default=0.1,
                        help='Минимальная задержка между запросами (по умолчанию 0.1)')
    parser.add_argument('--delay-max', type=float, default=1.0,
                        help='Максимальная задержка между запросами (по умолчанию 1.0)')
    parser.add_argument('--verify-ssl', action='store_true',
                        help='Включить проверку SSL-сертификатов (по умолчанию отключено)')
    parser.add_argument('--quiet', action='store_true',
                        help='Минимальный вывод в консоль')
    
    args = parser.parse_args()
    
    # Создаем объект парсера
    belgiss = BelgissParser(
        max_retries=args.retries,
        retry_delay=args.delay,
        verify_ssl=args.verify_ssl,
        max_threads=args.threads,
        delay_range=(args.delay_min, args.delay_max)
    )
    
    if not args.quiet:
        print("\n=== Парсер сертификатов/деклараций БелГИСС ===")
    
    # Режим с диапазоном дат
    if args.date_range:
        try:
            start_date = datetime.strptime(args.date_range[0], "%d.%m.%Y")
            end_date = datetime.strptime(args.date_range[1], "%d.%m.%Y")
            
            if end_date < start_date:
                print("Ошибка: дата окончания не может быть раньше даты начала")
                return
                
            if not args.quiet:
                print(f"\nПарсинг с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}")
            
            data = belgiss.parse_data_for_date_range(start_date, end_date, max_certs=args.max_certs)
                
            if data:
                if not args.output:
                    args.output = f"certifications_{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}.xlsx"
                belgiss.save_to_excel(data, args.output)
        except ValueError as e:
            print(f"Ошибка в формате даты: {e}")
            return
    
    # Режим с месяцем и годом
    elif args.month:
        if args.month < 1 or args.month > 12:
            print(f"Ошибка: {args.month} не является действительным месяцем (1-12)")
            return
            
        if not args.quiet:
            print(f"\nПарсинг за {calendar.month_name[args.month]} {args.year}")
        
        data = belgiss.parse_data_for_month(args.year, args.month, max_certs=args.max_certs)
            
        if data:
            if not args.output:
                args.output = f"certifications_{args.year}_{args.month:02d}.xlsx"
            belgiss.save_to_excel(data, args.output)
    
    # Интерактивный режим
    else:
        start_date, end_date = interactive_date_input()
        if not start_date or not end_date:
            return
        
        data = belgiss.parse_data_for_date_range(start_date, end_date, max_certs=args.max_certs)
        
        if data:
            if not args.output:
                args.output = f"certifications_{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}.xlsx"
            belgiss.save_to_excel(data, args.output)

if __name__ == "__main__":
    main()

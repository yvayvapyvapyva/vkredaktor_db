import os
import ydb
import ydb.iam
import json

# Инициализация YDB
endpoint = os.getenv("YDB_ENDPOINT")
database = os.getenv("YDB_DATABASE")

driver_config = ydb.DriverConfig(
    endpoint,
    database,
    credentials=ydb.iam.MetadataUrlCredentials()
)
driver = ydb.Driver(driver_config)

try:
    driver.wait(timeout=10)
except Exception as e:
    raise

pool = ydb.SessionPool(driver)

# --- YQL Запросы ---

def list_user_routes(session, id_param):
    """Получить список всех маршрутов (столбец m) для пользователя"""
    query = """
        DECLARE $id AS Utf8;
        SELECT m FROM roads WHERE id = $id;
    """
    prepared_query = session.prepare(query)
    return session.transaction().execute(prepared_query, {'$id': str(id_param)}, commit_tx=True)

def get_route_details(session, id_param, m_param):
    """Получить JSON конкретного маршрута"""
    query = """
        DECLARE $id AS Utf8;
        DECLARE $m AS Utf8;
        SELECT json FROM roads WHERE id = $id AND m = $m;
    """
    prepared_query = session.prepare(query)
    return session.transaction().execute(
        prepared_query,
        {'$id': str(id_param), '$m': str(m_param)},
        commit_tx=True
    )

def delete_route(session, id_param, m_param):
    """Удалить конкретный маршрут"""
    query = """
        DECLARE $id AS Utf8;
        DECLARE $m AS Utf8;
        DELETE FROM roads WHERE id = $id AND m = $m;
    """
    prepared_query = session.prepare(query)
    return session.transaction().execute(
        prepared_query,
        {'$id': str(id_param), '$m': str(m_param)},
        commit_tx=True
    )

def upsert_route(session, id_param, m_param, json_data):
    """Создать или перезаписать маршрут (UPSERT)"""
    query = """
        DECLARE $id AS Utf8;
        DECLARE $m AS Utf8;
        DECLARE $json AS Json;
        UPSERT INTO roads (id, m, json) VALUES ($id, $m, $json);
    """
    prepared_query = session.prepare(query)
    return session.transaction().execute(
        prepared_query,
        {
            '$id': str(id_param),
            '$m': str(m_param),
            '$json': json.dumps(json_data) if not isinstance(json_data, str) else json_data
        },
        commit_tx=True
    )

def update_route_meta(session, id_param, m_param, name, description, visible):
    """Обновить метаданные маршрута (name, description, visible)"""
    query = """
        DECLARE $id AS Utf8;
        DECLARE $m AS Utf8;
        DECLARE $name AS Utf8;
        DECLARE $description AS Utf8;
        DECLARE $visible AS Bool;
        UPDATE roads SET name = $name, description = $description, visible = $visible WHERE id = $id AND m = $m;
    """
    prepared_query = session.prepare(query)
    return session.transaction().execute(
        prepared_query,
        {
            '$id': str(id_param),
            '$m': str(m_param),
            '$name': str(name),
            '$description': str(description),
            '$visible': bool(visible)
        },
        commit_tx=True
    )

def get_route_meta(session, id_param, m_param):
    """Получить метаданные маршрута (name, description, visible)"""
    query = """
        DECLARE $id AS Utf8;
        DECLARE $m AS Utf8;
        SELECT name, description, visible FROM roads WHERE id = $id AND m = $m;
    """
    prepared_query = session.prepare(query)
    return session.transaction().execute(
        prepared_query,
        {'$id': str(id_param), '$m': str(m_param)},
        commit_tx=True
    )

# --- Вспомогательные функции ---

def create_response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type'
        },
        'body': json.dumps(body, ensure_ascii=False)
    }

# --- Основной хендлер ---

def handler(event, context):
    params = event.get('queryStringParameters', {})
    method = event.get('httpMethod')
    body = event.get('body', '')

    # Обработка CORS preflight запроса
    if method == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type'
            },
            'body': ''
        }

    # Извлекаем параметры
    action = params.get('action', 'get') # list, get, delete, save
    id_val = params.get('id')
    m_val = params.get('m')

    if not id_val:
        return create_response(400, {'error': 'missing_user_id'})

    try:
        # 1. Получение списка маршрутов пользователя
        if action == 'list':
            result = pool.retry_operation_sync(list_user_routes, id_param=id_val)
            routes = [row.m for row in result[0].rows]
            return create_response(200, {'routes': routes})

        # 2. Получение данных конкретного маршрута
        elif action == 'get':
            if not m_val: return create_response(400, {'error': 'missing_route_name'})
            result = pool.retry_operation_sync(get_route_details, id_param=id_val, m_param=m_val)
            if not result[0].rows:
                return create_response(404, {'error': 'route_not_found'})

            raw_data = result[0].rows[0].json
            try:
                parsed_data = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
            except:
                parsed_data = []
            return create_response(200, {'id': id_val, 'm': m_val, 'data': parsed_data})

        # 3. Удаление маршрута
        elif action == 'delete':
            if not m_val: return create_response(400, {'error': 'missing_route_name'})
            pool.retry_operation_sync(delete_route, id_param=id_val, m_param=m_val)
            return create_response(200, {'status': 'deleted'})

        # 4. Перезапись/Сохранение (обычно через POST/PUT, но сделаем через action для простоты)
        elif action == 'save':
            if not m_val: return create_response(400, {'error': 'missing_route_name'})

            # Данные для записи берем из body
            body_str = event.get('body', '{}')

            try:
                new_json = json.loads(body_str) if body_str else []
            except Exception as je:
                return create_response(400, {'error': 'invalid_json_body', 'details': str(je)})

            try:
                pool.retry_operation_sync(upsert_route, id_param=id_val, m_param=m_val, json_data=new_json)
            except Exception as se:
                raise

            return create_response(200, {'status': 'saved'})

        # 5. Получение метаданных маршрута
        elif action == 'get_meta':
            if not m_val: return create_response(400, {'error': 'missing_route_name'})
            result = pool.retry_operation_sync(get_route_meta, id_param=id_val, m_param=m_val)
            if not result[0].rows:
                return create_response(404, {'error': 'route_not_found'})
            row = result[0].rows[0]
            return create_response(200, {
                'name': row.name if hasattr(row, 'name') else '',
                'description': row.description if hasattr(row, 'description') else '',
                'visible': row.visible if hasattr(row, 'visible') else False
            })

        # 6. Сохранение метаданных маршрута
        elif action == 'save_meta':
            if not m_val: return create_response(400, {'error': 'missing_route_name'})

            try:
                body_data = json.loads(body) if body else {}
            except Exception as je:
                return create_response(400, {'error': 'invalid_json_body', 'details': str(je)})

            name = body_data.get('name', '')
            description = body_data.get('description', '')
            visible = body_data.get('visible', False)

            try:
                pool.retry_operation_sync(update_route_meta, id_param=id_val, m_param=m_val, name=name, description=description, visible=visible)
            except Exception as se:
                raise

            return create_response(200, {'status': 'meta_saved'})

        else:
            return create_response(400, {'error': 'unknown_action'})

    except ValueError as ve:
        return create_response(400, {'error': 'invalid_parameter_format', 'details': str(ve)})
    except Exception as e:
        return create_response(500, {'error': 'internal_server_error', 'details': str(e)})

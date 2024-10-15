from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3

app = Flask(__name__)
CORS(app)
DATABASE = 'shop.db'
table_schemes: dict = {}

## Initialization

def init_db():
    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS products (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL UNIQUE, quantity INTEGER)")
        cursor.execute("CREATE TABLE IF NOT EXISTS clients (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL UNIQUE)")
        cursor.execute("CREATE TABLE IF NOT EXISTS transactions (transaction_id INTEGER NOT NULL PRIMARY KEY, transaction_date TEXT DEFAULT CURRENT_DATE, product_id INTEGER, product_name TEXT, quantity INTEGER, client_id INTEGER, client_name TEXT, type_of_transaction TEXT)")
        conn.commit()
    
        cursor = conn.cursor()
        list_of_tables = cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        table_names = [table[0] for table in list_of_tables]
        for name in table_names[1:]:
            result_sql = cursor.execute(f"PRAGMA table_info({name})").fetchall()
            table_scheme: list = []
            for entry in result_sql:
                table_scheme.append((entry[1], entry[2]))
            table_schemes[name] = table_scheme

#########################################################################
#########################################################################
## Tools

## Verify

# Verify user information

def verify_user(json, admin_required = False) -> str | dict:
    if not 'user' in json:
        return {"error": "User required"}
    try:
        user = json["user"].lower()
        if user == "": 
            raise ValueError
        if admin_required and user != 'admin': 
            return {"error": "Unauthorized user"}
    except: return {"error": "Invalid user information"}
    else: return user

# Verify json_data validity

def verify_json_str_values(json_dict: dict, key: str):
    try:
        json_dict[key] = str(json_dict[key].lower())
    except Exception: 
        return ({"error": f"{key.capitalize()} must be a string", key: json_dict[key]})
    
def verify_json_int_values(json_dict: dict, key: str):
    try:
        json_dict[key] = int(json_dict[key])
        json_dict[key] > 1
    except Exception: 
        return ({"error": f"{key.capitalize()} must be an integer and strictly positive", key: json_dict[key]})

def verify_json_data(json_list: list, mandatory_keys: list, **other_keys: list) -> None | list:
    for json_dict in json_list:
        for key in mandatory_keys:
            if key in json_dict:
                continue
            return ({"error": f"{key.capitalize()} required", key: json_dict[key]})
        
        if "semi_mandatory_keys" in other_keys:
            for index, key in enumerate(other_keys["semi_mandatory_keys"]):
                if key in json_dict:
                    break
                if index < len(other_keys["semi_mandatory_keys"]) - 1:
                    continue
                semi_mandatory_keys = [*other_keys["semi_mandatory_keys"]]
                return ({"error": f"{' and '.join(semi_mandatory_keys).capitalize()} required", key: json_dict[key]})
        
        if 'str_keys' in other_keys:
            for key in other_keys['str_keys']:
                if key not in json_dict:
                    continue
                if error := verify_json_str_values(json_dict, key):
                    return error

        if 'int_keys' in other_keys:
            for key in other_keys['int_keys']:
                if key not in json_dict:
                    continue
                if error := verify_json_int_values(json_dict, key):
                    return error

# Verify potential name and new name conflict

def identical_names(json_data, original_i, original_dict, duplicate_i, duplicate_dict) -> None | dict:
    if original_dict["name"] != duplicate_dict["name"]:
        return None
    if "new name" in original_dict:
        if "new name" in duplicate_dict and original_dict["new name"] != duplicate_dict["new name"]:
            return {"error": "Duplicate name, please resolve conflict"}, original_dict, duplicate_dict
    if "new name" in duplicate_dict:
        original_dict["new name"] = duplicate_dict["new name"]
    if "new quantity" in duplicate_dict:
        try:
            original_dict["new quantity"] += duplicate_dict["new quantity"]
        except Exception:
            original_dict["new quantity"] = duplicate_dict["new quantity"]
    del json_data[original_i + duplicate_i + 1]

def identical_new_names(original_dict, duplicate_dict) -> None | dict:
    if "new name" not in duplicate_dict:
        return None
    if original_dict["new name"] != duplicate_dict["new name"]:
        return None
    if original_dict["name"] == duplicate_dict["name"]:
        return None
    return {"error": "New name duplicate found"}, original_dict, duplicate_dict

def name_vs_new_name(original_dict, duplicate_dict) -> None | dict:
    if original_dict["new name"] != duplicate_dict["name"]:
        return None
    if original_dict["name"] == duplicate_dict["name"]:
        return None
    return {"error": "New name cannot match a product's name"}, original_dict, duplicate_dict

def verify_potential_name_conflicts(json_data):
    for original_i, original_dict in enumerate(json_data):
        for duplicate_i, duplicate_dict in enumerate(json_data[original_i + 1:]):
            if error := identical_names(json_data, original_i, original_dict, duplicate_i, duplicate_dict):
                return error
            if "new name" in original_dict and (error := identical_new_names(original_dict, duplicate_dict)):
                return error
        for duplicate_dict in json_data:
            if "new name" in original_dict and (error := name_vs_new_name(original_dict, duplicate_dict)):
                return error

# Verify value types for the view request

def verify_value_types_and_adjust(filter_data, key, column_type):
    if "TEXT" in column_type:
        if error := verify_json_str_values(filter_data, key):
            return error
    if "INTEGER" in column_type:
        if error := verify_json_int_values(filter_data, key):
            return error

## Retrieve

# Retrieve name duplicates from sql

def duplicates_from_sql(dict_list: list, *json_dict_keys: str, table: str):
    acc_list: list = []
    for key in json_dict_keys:
        current = [json_dict[key] for json_dict in dict_list if key in json_dict]
        acc_list += current

    json_tuple_product_names: tuple = tuple(acc_list) 
    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        return cursor.execute("SELECT * FROM {0} WHERE name IN ({1})".format(table, ', '.join('?' for _ in json_tuple_product_names)), json_tuple_product_names).fetchall()

# Retrieve user id

def user_info(client_name: str) -> tuple:
    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        result = cursor.execute("SELECT * FROM clients WHERE name = ?", (client_name,)).fetchone()
    if not result: 
        return {"error": "User not found"}
    return result

## Delete

# Delete duplicates in the same list

def delete_name_duplicates_in_list(dict_list: list, key_to_match: str, *keys_to_adjust: list) -> None:
    for original_i, original_dict in enumerate(dict_list):
        duplicate_i = original_i + 1
        while duplicate_i < len(dict_list):
            if original_dict[key_to_match] != dict_list[duplicate_i][key_to_match]:
                duplicate_i += 1
                continue
            for key in keys_to_adjust:
                if key not in dict_list[duplicate_i]:
                    continue
                try:
                    original_dict[key] += dict_list[duplicate_i][key]
                except Exception:
                    original_dict[key] = dict_list[duplicate_i][key]
            del dict_list[duplicate_i]

# Delete matching values between two lists

def delete_multiple_lists_comparison(dict_list: list, sql_list: list, key_to_match: str) -> list:
    json_list_duplicates: list = []
    for sql_tuple in sql_list:
        for index, json_dict in enumerate(dict_list):
            if key_to_match not in json_dict or sql_tuple[1] != json_dict[key_to_match]:
                continue
            json_dict["id"] = sql_tuple[0]
            if len(sql_tuple) > 2:
                json_dict["quantity"] = sql_tuple[2]
            json_list_duplicates.append(json_dict)
            del dict_list[index]
            break
    return json_list_duplicates

## Conversion

# Accumulator

def accumulator_for_buy_return(transaction_list: list, type_of_transaction: str) -> int:
    acc: int = 0
    for sql_tuple in transaction_list:
        if type_of_transaction in sql_tuple[1]:
            acc += sql_tuple[0]
    return acc

# To json

def display_in_json(result_list: list, table: str, descending: bool = False, error: dict = None) -> list:
    list_to_display: list = []
    for result_el in reversed(result_list) if descending else result_list:
        if "search" in result_el:
            list_to_display.append(result_el)
            continue
        new_dict: dict = {name[0]: result_el[index] for index, name in enumerate(table_schemes[table])}
        list_to_display.append(new_dict)
    return error + list_to_display if error else list_to_display

# Tuple int to str

def convert_tuple_int_to_string(sql_results: list, column_nr: int = 0) -> list:
    new_sql_results: list = []
    for sql_tuple in sql_results:
        new_tuple = tuple(str(el) if i == column_nr else el for i, el in enumerate(sql_tuple))
        new_sql_results.append(new_tuple)
    return new_sql_results

## Binary

# B-sort

def b_sort_sql_results(sql_results: list, column_nr: int = 0) -> list:
    left: list = [el for el in sql_results[:int(len(sql_results) / 2)]]
    right: list = [el for el in sql_results[int(len(sql_results) / 2):]]
    if len(left) > 1: left = b_sort_sql_results(left, column_nr)
    if len(right) > 1: right = b_sort_sql_results(right, column_nr)

    li: int = 0
    ri: int = 0
    sorted_list: list = []

    while li < len(left) and ri < len(right):
        if left[li][column_nr] < right[ri][column_nr]:
            sorted_list.append(left[li])
            li += 1
        elif left[li][column_nr] > right[ri][column_nr]:
            sorted_list.append(right[ri])
            ri += 1
        else:
            sorted_list.append(left[li])
            sorted_list.append(right[ri])
            li += 1
            ri += 1
    
    while li < len(left) or ri < len(right):
        if li < len(left):
            sorted_list.append(left[li])
            li += 1
        elif ri < len(right):
            sorted_list.append(right[ri])
            ri += 1

    return sorted_list

# Search additional matching values after B-search

def search_additional_matching_values(mid: int, sql_results: list, data, column_nr: int = 0) -> int:
    left = mid - 1
    right = mid + 1

    while left >= 0 and data == sql_results[left][column_nr]: 
        left -= 1
    left += 1

    while right < len(sql_results) and data == sql_results[right][column_nr]: 
        right += 1
    right -= 1

    return left, right

# B-search

def b_search_sql_results(list: list, data, column_nr: int = 0) -> int:
    left = 0
    right = len(list) - 1

    while right >= left:
        mid = int((left + right) / 2)
        if data == list[mid][column_nr]:
            return mid
        if data > list[mid][column_nr]:
            left = mid + 1
        if data < list[mid][column_nr]:
            right = mid - 1
    return mid

## View filters

# = Equal / <= Greater / >= Less

def filter_comparison(sql_results: list, data, column_nr = 0, key = "==" or "<=" or ">=") -> list:
    filtered_list: list = []
    if len(sql_results) == 1: 
        data == sql_results[0][column_nr] and filtered_list.append(sql_results[0])
        return filtered_list
    
    f_range: range = range(0)
    mid = b_search_sql_results(sql_results, data, column_nr)
    left, right = search_additional_matching_values(mid, sql_results, data, column_nr)
    
    match key:
        case "==":
            if data != sql_results[mid][column_nr]:
                return None
            f_range = range(left, right + 1)
        case "<=":
            f_range = range(left, len(sql_results)) if data <= sql_results[left][column_nr] else range(left + 1, len(sql_results))
        case ">=":
            f_range = range(0, right + 1) if data >= sql_results[right][column_nr] else range(0, right)
    
    filtered_list = [sql_results[index] for index in f_range]
    return filtered_list

# -- In between

def filter_in_between(sql_results: list, data, second_data, column_nr = 0) -> list:
    filtered_list: list = []
    rev_check: bool = True if second_data < data else False
    if rev_check: data, second_data = second_data, data

    mid = b_search_sql_results(sql_results, data, column_nr)
    left, _ = search_additional_matching_values(mid, sql_results, data, column_nr)
    if data > sql_results[left][column_nr]:
        left += 1

    mid = b_search_sql_results(sql_results, second_data, column_nr)
    _, right = search_additional_matching_values(mid, sql_results, second_data, column_nr)
    if data >= sql_results[right][column_nr]:
        right += 1

    filtered_list = [sql_results[index] for index in range(left, right)]
    return filtered_list

# ** Contains

def filter_contains(sql_results: list, data: str, column_nr = 0) -> list:
    i: int = 0
    filtered_list: list = []
    
    for el in sql_results:
        data_i: int = 0
        sql_i: int = 0
        while data_i < len(data) and sql_i < len(el[column_nr]):
            if data[data_i] == el[column_nr][sql_i]:
                data_i += 1
            elif sql_i >= len(el[column_nr]) - len(data):
                break
            else:
                data_i = 0
            sql_i += 1
        if data_i == len(data):
            filtered_list.append(sql_results[i])
        i += 1
    
    return filtered_list

# *a Starts with / a* Ends with

def filter_start_end(sql_results: list, data: str, column_nr = 0, key = "*a" or "a*") -> list:
    i: int = 0
    filtered_list: list = []

    for el in sql_results:
        ch_i: int = 0 if "*a" in key else -1
        counter: int = 0
        if len(el[column_nr]) >= len(data) and el[column_nr][ch_i] == data[ch_i]:
            while counter < len(el[column_nr]) and counter < len(data):
                ch_i = ch_i + 1 if "*a" in key else ch_i - 1
                counter += 1
            if counter == len(data):
                filtered_list.append(el)    
        i += 1

    return filtered_list

# Filter Handler

def filter_list(sql_results: list, data: str | int, column_nr: int, operator: str = None, second_data: str | int = None) -> list:
    if len(sql_results) > 1:
        sql_results = b_sort_sql_results(sql_results, column_nr)
    
    filtered_list: list = []
    if not operator:
        filtered_list = filter_comparison(sql_results, data, column_nr, "==")
        if isinstance(sql_results[0][column_nr], str) and len(data) >= 3:
            data = str(data).lower()
            if result := filter_start_end(sql_results, data, column_nr, "*a"):
                filtered_list += result
        if isinstance(sql_results[0][column_nr], str) and len(data) >= 5:
            data = str(data).lower()
            if result := filter_contains(sql_results, data, column_nr):
                filtered_list += result
        if not filtered_list:
            return {"error": "Information not found with the search criteria"}
        return filtered_list

    match operator:
        case "==" | ">=" | "<=":
            filtered_list = filter_comparison(sql_results, data, column_nr, operator)
        case "**":
            if isinstance(sql_results[0][column_nr], int):
                sql_results = convert_tuple_int_to_string(sql_results, column_nr)
                data = str(data)
            filtered_list = filter_contains(sql_results, data, column_nr)
        case "*a" | "a*":
            if isinstance(sql_results[0][column_nr], int):
                sql_results = convert_tuple_int_to_string(sql_results, column_nr)
                data = str(data)
            filtered_list = filter_start_end(sql_results, data, column_nr, operator)
        case "--":
            filtered_list = filter_in_between(sql_results, data, second_data, column_nr)
        case _:
            return {"error": "Invalid operator"}
        
    if not filtered_list:
        return {"error": "Information not found with the search criteria"}
    return filtered_list

# Handle request for one or more items in a single dictionary and accumulate all results if needed

def verify_filters(sql_results: list, json_filter: dict, table: str):
    result: list = sql_results
    for key in json_filter:
        for index, table_tuple in enumerate(table_schemes[table]):
            column_name, column_type = table_tuple
            if key not in column_name:
                continue

            filter_data: dict = {}
            if isinstance(json_filter[key], list):
                if len(json_filter[key]) == 1:
                    filter_data[key] = json_filter[key][0]
                    filter_data['operator'] = None
                if len(json_filter[key]) == 2:
                    filter_data[key], filter_data['operator'] = json_filter[key]
                filter_data['second data'] = None
                if len(json_filter[key]) == 3 and "--" in json_filter[key]:
                    filter_data[key], filter_data['second data'], filter_data['operator'] = json_filter[key]
                    if error := verify_value_types_and_adjust(filter_data, 'second data', column_type):
                        return None, error
                elif len(json_filter[key]) != 3 and "--" in json_filter[key]:
                    return None, {"error": "The operator requires a second value", key: json_filter[key][0], "operator": json_filter[key][1]}
                elif len(json_filter[key]) > 2:
                    return None, {"error": "Innapropriate search format, one value and one operator, except for '--', where two search values of the same type and one operator are required", key: json_filter[key][0], "operator": json_filter[key][1]}
            else:
                filter_data[key] = json_filter[key]
                filter_data['second data'] = None
                filter_data['operator'] = None
            
            if error := verify_value_types_and_adjust(filter_data, key, column_type):
                return None, error

            result = filter_list(result, filter_data[key], index, filter_data['operator'], filter_data['second data'])
            if "error" in result:
                filter_data["error"] = result["error"]
                return None, filter_data
    return result, None

# Narrow down result based on multiple json dictionary request

def multiple_results(sql_results: list, json_filter_list: list, table: str) -> list:
    acc_results: list = []
    acc_error: list = []
    for index, json_filter in enumerate(json_filter_list):
        results, error = verify_filters(sql_results, json_filter, table)
        if results:
            acc_results += [{"search": index + 1}] + results
        if error:
            acc_error.append(error)
    return acc_results, *acc_error

# Reorder view based on json request

def order_by_column(result_list: list, json_order: dict, table: str) -> list:
    for index, column_name in enumerate(table_schemes[table]):
        if json_order["column"] == column_name[0]:
            column_nr = index
    return b_sort_sql_results(result_list, column_nr)

# Check if the json is a list of dictionaries or a dictionary

def view_filter(sql_results: list, json: dict, table: str) -> list:
    json_filter = json["filter"]
    if isinstance(json_filter, list):
        json_filter[0]
        result_list = multiple_results(sql_results, json_filter, table)
    else:
        result_list = verify_filters(sql_results, json_filter, table)
    return result_list

# Handle json request and sent info to the right function

def view_handler(json: dict, result_list: list, table: str):
    if "filter" in json:
        result_list, error = view_filter(result_list, json, table)
    else:
        result_list = None
        error = None
            
    if not result_list:
        return error

    if "order" in json:
        json_order = json["order"]
        if "column" in json["order"]:
            result_list = order_by_column(result_list, json_order, table)
    try:
        descending = json_order["descending"]
    except Exception:
        descending = False
    
    return display_in_json(result_list, table, descending, error)

#########################################################################
#########################################################################
## Products Routes

## Add

@app.route("/api/products/add", methods = ["POST"])
def add_product():
    json = request.get_json()
    user = verify_user(json, admin_required = True)
    if 'error' in user: 
        return jsonify(user), 401
    
    json_data = json["data"]
    if not json_data: 
        return jsonify({"error": "No information entered"}), 400
    
    if error := verify_json_data(json_data, mandatory_keys = ['name', 'quantity'], str_keys = ['name'], int_keys = ['quantity']):
        return jsonify(*error), 400
        
    len(json_data) > 1 and delete_name_duplicates_in_list(json_data, "name", "quantity")
    sql_product_duplicates = duplicates_from_sql(json_data, "name", table = "products")
    if len(json_data) == len(sql_product_duplicates):
        return jsonify({"error": "Task failed, all duplicates"}), 400
    json_data_duplicates = delete_multiple_lists_comparison(json_data, sql_product_duplicates, "name")

    json_list_products: list = [(json_dict["name"], json_dict["quantity"]) for json_dict in json_data]
    json_list_transactions: list = [(json_dict["name"], json_dict["name"], json_dict["quantity"]) for json_dict in json_data]

    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        cursor.executemany("INSERT INTO products (name, quantity) VALUES (?, ?)", json_list_products)
        cursor.executemany("INSERT INTO transactions (product_id, product_name, quantity, client_id, client_name, type_of_transaction) VALUES ((SELECT id FROM products WHERE name = ?), ?, ?, '0', 'admin', 'add')", json_list_transactions)
        conn.commit()
        
    if not sql_product_duplicates:
        return jsonify({"message": "Task successful"}), 201
    else:
        return jsonify({"message": "Task partially successful, duplicates found"}, {"Successful": json_data}, {"Duplicates in database": json_data_duplicates}), 201
        
## Remove

@app.route("/api/products/remove", methods = ["POST"])
def remove_product():
    json = request.get_json()
    user = verify_user(json, admin_required = True)
    if 'error' in user: 
        return jsonify(user), 401
    
    json_data = json["data"]
    if not json_data: 
        return jsonify({"error": "No information entered"}), 400

    if error := verify_json_data(json_data, mandatory_keys = ['name'], str_keys = ['name'], int_keys = ['quantity']):
        return jsonify(*error), 400

    len(json_data) > 1 and delete_name_duplicates_in_list(json_data, "name")
    sql_product_duplicates = duplicates_from_sql(json_data, "name", table = "products")
    if not sql_product_duplicates:
        return jsonify({"error": "No match found with any of the product names"}), 404
    
    sql_tuple_products: list = [(sql_tuple[0],) for sql_tuple in sql_product_duplicates]
    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        cursor.executemany("DELETE FROM products WHERE id = ?", sql_tuple_products)
        cursor.executemany("INSERT INTO transactions (product_id, product_name, quantity, client_id, client_name, type_of_transaction) VALUES (?, ?, ?, '0', 'admin', 'remove')", sql_product_duplicates)
        conn.commit()

    if len(json_data) == len(sql_product_duplicates):
        return jsonify({"message": "Product/s deleted successfully"}), 201
    
    json_data_duplicates = delete_multiple_lists_comparison(json_data, sql_product_duplicates, key_to_match = "name")
    return jsonify({"message": "Deletion partially succesful"}, {"Successful": json_data_duplicates}, {"No match found": json_data}), 201

## Edit

@app.route("/api/products/edit", methods = ["POST"])
def edit_product():
    json = request.get_json()
    user = verify_user(json)
    if 'error' in user: 
        return jsonify(user), 401
    
    json_data = json["data"]
    if not json_data: 
        return jsonify({"error": "No information entered"}), 400
    
    if "admin" in user:
        if error := verify_json_data(json_data, mandatory_keys = ['name'], semi_mandatory_keys = ['new name', 'new quantity'], str_keys = ['name', 'new name'], int_keys = ['new quantity']):
            return jsonify(*error), 400
        if error := verify_potential_name_conflicts(json_data): 
            return jsonify(*error), 400
        
        sql_name_duplicates = duplicates_from_sql(json_data, "name", "new name", table = "products")
        json_match_list = delete_multiple_lists_comparison(json_data, sql_name_duplicates, key_to_match = 'name')
        json_skip_list = delete_multiple_lists_comparison(json_match_list, sql_name_duplicates, key_to_match = 'new name')
        if not json_match_list:
            return ({"error": "No match found with any of the product names"}), 404

        new_name_products: list = []
        new_quantity_products: list = []
        for json_dict in json_match_list:
            if 'new quantity' in json_dict:
                if json_dict['quantity'] == json_dict['new quantity']:
                    del json_dict['new quantity']
                else:
                    new_quantity_products.append((json_dict['new quantity'], json_dict['id']))
                    if 'new name' not in json_dict:
                        json_dict['transaction'] = 'update quantity'
                        continue
                    json_dict['transaction'] = 'update name and quantity'
            if "new name" in json_dict:
                new_name_products.append((json_dict['new name'], json_dict['id']))
                if 'new quantity' not in json_dict:
                    json_dict['transaction'] = 'update name'
                    continue
                json_dict['transaction'] = 'update name and quantity'
        
        transaction_list: list = [(json_dict['id'], json_dict['new name'] if 'new name' in json_dict else json_dict['name'], json_dict['new quantity'] if 'new quantity' in json_dict else json_dict['quantity'], json_dict['transaction']) for json_dict in json_match_list if 'transaction' in json_dict]

        with sqlite3.connect(DATABASE) as conn:
            cursor = conn.cursor()
            cursor.executemany("UPDATE products SET name = ? WHERE id = ?", new_name_products)
            cursor.executemany("UPDATE products SET quantity = ? WHERE id = ?", new_quantity_products)
            cursor.executemany("INSERT INTO transactions (product_id, product_name, quantity, client_id,client_name, type_of_transaction) VALUES (?, ?, ?, '0', 'admin', ?)", transaction_list)
            conn.commit()

        if json_skip_list and json_data:
            message_list: list = [{"message": "Update partially succesful"}, {"Successful": [json_dict for json_dict in json_match_list if 'transaction' in json_dict]}]
            if json_data: 
                message_list.append({"No match for name found": json_data})
            if json_skip_list: 
                message_list.append({"New name not unique": json_skip_list})
            return jsonify(*message_list), 201
        return jsonify({"message": "Product information changed successfully"}), 201

    else:
        with sqlite3.connect(DATABASE) as conn:
            cursor = conn.cursor()
            client = cursor.execute("SELECT id FROM clients WHERE name = ?", (user,)).fetchone()
            if client == None:
                return jsonify({"error": "Client not found"})
            else:
                client_id = client[0]
        
        if error := verify_json_data(json_data, mandatory_keys = ['name'], semi_mandatory_keys = ['buy', 'return'], str_keys = ['name'], int_keys = ['buy', 'return']): 
            return jsonify(*error), 400
        
        len(json_data) > 1 and delete_name_duplicates_in_list(json_data, 'name', 'buy', 'return')

        sql_name_duplicates = duplicates_from_sql(json_data, "name", table = "products")
        json_match_list = delete_multiple_lists_comparison(json_data, sql_name_duplicates, 'name')
        if not json_match_list:
            return jsonify({"error": "No match found with any of the product names"}), 404
        
        transactions_list_select: tuple = (client_id, *(json_dict["id"] for json_dict in json_match_list if 'return' in json_dict))
        with sqlite3.connect(DATABASE) as conn:
            cursor = conn.cursor()
            sql_results = cursor.execute("SELECT * FROM transactions WHERE client_id = ? AND product_id IN ({0}) AND (type_of_transaction = 'buy' OR type_of_transaction = 'return')".format(' ,'.join('?' for _ in transactions_list_select[1:])), transactions_list_select).fetchall()

        sql_transactions_result: list = [{'id': sql_tuple[2], sql_tuple[7]: sql_tuple[4]} for sql_tuple in sql_results]
        len(sql_transactions_result) > 1 and delete_name_duplicates_in_list(sql_transactions_result, 'id', 'buy', 'return')

        products_list: list = []
        transactions_insert: list = []

        for json_dict in json_match_list:
            if 'buy' in json_dict:
                if json_dict['buy'] > json_dict['quantity']:
                    return jsonify({"error": "Not enough in stock to complete transaction"}, json_dict)
                
                json_dict['quantity'] -= json_dict['buy']
                json_dict['transaction'] = 'buy'
                transactions_insert.append((json_dict['id'], json_dict['name'], json_dict['buy'], client_id, user, json_dict['transaction']))

            if 'return' in json_dict:
                transaction_dict, *_ = [sql_dict for sql_dict in sql_transactions_result if sql_dict['id'] == json_dict['id']]
                if not transaction_dict:
                    continue
                if 'return' in transaction_dict:
                    if transaction_dict['buy'] - transaction_dict['return'] < json_dict['return']:
                        return jsonify({"error": "Return amount too high"}, json_dict, transaction_dict)
                else:
                    if transaction_dict['buy'] < json_dict['return']:
                        return jsonify({"error": "Return amount too high"}, json_dict, transaction_dict)
                    
                json_dict['quantity'] += json_dict['return']
                json_dict['transaction'] = 'return'
                transactions_insert.append((json_dict['id'], json_dict['name'], json_dict['return'], client_id, user, json_dict['transaction']))
            products_list.append((json_dict['quantity'], json_dict['id']))
        
        with sqlite3.connect(DATABASE) as conn:
            cursor = conn.cursor()
            cursor.executemany("UPDATE products SET quantity = ? WHERE id = ?", products_list)
            cursor.executemany("INSERT INTO transactions (product_id, product_name, quantity, client_id, client_name, type_of_transaction) VALUES (?, ?, ?, ?, ?, ?)", transactions_insert)
            conn.commit()

        if json_data:
            return jsonify ({"message": "Transaction partially succesful"}, {"Successful": json_match_list}, {"No match for name found": json_data})
        return jsonify({'message': "Transaction succesful"}), 200

## View

@app.route("/api/products", methods = ["GET"])
def view_products():
    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        result_list = cursor.execute("SELECT * FROM products").fetchall()
    if len(result_list) < 1: 
        return jsonify({"message": "Table empty"}), 404
    
    json = request.get_json()
    if not json:
        return jsonify(display_in_json(result_list, "products"))

    return jsonify(view_handler(json, result_list, 'products')), 200

#########################################################################
#########################################################################
## Clients routes

## Add

@app.route("/api/clients/add", methods = ["POST"])
def add_client():
    json = request.get_json()
    json_data = json["data"]
    if not json_data: 
        return jsonify({"error": "No information entered"}), 400
    if len(json_data) > 1 and not 'user' in json and not 'admin' in json['user'].lower():
        return({"error": "Not authorized to create more than one account"}), 401
    
    if error := verify_json_data(json_data, mandatory_keys = ['name'], str_keys = ['name']):
        return jsonify(*error), 400

    len(json_data) > 1 and delete_name_duplicates_in_list(json_data, 'name')
    sql_name_duplicates = duplicates_from_sql(json_data, "name", table = "clients")
    if len(json_data) == len(sql_name_duplicates):
        return jsonify({"error": "Task failed, all duplicates"}), 400
    json_data_duplicates = delete_multiple_lists_comparison(json_data, sql_name_duplicates, 'name')

    client_insert: list = [(json_dict['name'],) for json_dict in json_data]
    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        cursor.executemany("INSERT INTO clients (name) VALUES (?)", client_insert)
        conn.commit()
    
    if not sql_name_duplicates:
        return jsonify({"message": "Task successful"}), 201
    else:
        return jsonify({"message": "Task partially successful, duplicates found"}, {"Successful": json_data}, {"Duplicates in database": json_data_duplicates}), 201

## Remove

@app.route("/api/clients/remove", methods = ["POST"])
def remove_client():
    json = request.get_json()
    user = verify_user(json)
    if 'error' in user: 
        return jsonify(user), 401

    if "admin" in user:
        json_data = json["data"]
        if error := verify_json_data(json_data, mandatory_keys = ['name'], str_keys = ['name']):
            return jsonify(*error), 400
        len(json_data) > 1 and delete_name_duplicates_in_list(json_data, 'name')
        sql_name_duplicates = duplicates_from_sql(json_data, 'name', table = "clients")
        if not sql_name_duplicates:
            return jsonify({"error": "No match found with any of the client names"}), 404
        clients = [(sql_tuple[0]) for sql_tuple in sql_name_duplicates]
    else:
        clients = user_info(user)
        if 'error' in clients: 
            return jsonify(clients), 404
        else:
            clients = [(clients[0],)]

    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        cursor.executemany("DELETE FROM clients WHERE id = ?", clients)
        conn.commit()

    if "admin" in user and len(json) == len(sql_name_duplicates):
        return jsonify({"message": "Client information deleted successfully"}), 201

    if not "admin" in user:
        return jsonify({"message": "Task successful"})

    json_data_duplicates = delete_multiple_lists_comparison(json_data, sql_name_duplicates, "name")
    return jsonify({"message": "Deletion partially succesful"}, {"Successful": json_data_duplicates}, {"No match found": json_data}), 201

## Edit

@app.route("/api/clients/edit", methods = ["POST"])
def edit_client():
    json = request.get_json()
    user = verify_user(json)
    if 'error' in user: 
        return jsonify(user), 401

    json_data = json["data"]
    if "admin" in user:
        if error := verify_json_data(json_data, mandatory_keys = ['name', 'new name'], str_keys = ['name', 'new name']):
            return jsonify(*error), 400
        sql_name_duplicates = duplicates_from_sql(json_data, 'name', 'new name', table = "clients")
        if not sql_name_duplicates:
            return jsonify({"error": "No match found with any of the client names"}), 404
        
        verify_potential_name_conflicts(json_data)
        sql_name_duplicates = duplicates_from_sql(json_data, "name", "new name", table = "clients")
        json_match_list = delete_multiple_lists_comparison(json_data, sql_name_duplicates, key_to_match = 'name')
        json_skip_list = delete_multiple_lists_comparison(json_match_list, sql_name_duplicates, key_to_match = 'new name')
        if not json_match_list:
            return ({"error": "No match found with any of the client names"}), 404
        clients = [(json_dict['new name'], json_dict['id']) for json_dict in json_match_list]

    else:
        client_id = user_info(user)
        if 'error' in client_id: 
            return jsonify(client_id), 404
        else:
            client_id = client_id[0]
        if len(json_data) > 1:
            return jsonify({"error": "Please choose only one new name"}), 400 
        if error := verify_json_data(json_data, mandatory_keys = ['new name'], str_keys = ['new name']):
            return jsonify(*error), 400
        clients = [(json_dict['new name'], client_id) for json_dict in json_data] if type(json_data) == list else (json_data['new name'], client_id)

    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        cursor.executemany("UPDATE clients SET name = ? WHERE id = ?", clients)
        conn.commit()
    
    if 'admin' not in user or (not json_skip_list and not json_data):
        return jsonify({"message": "Client information changed successfully"}), 201
    if json_skip_list or json_data:
            message_list: list = [{"message": "Update partially succesful"}, {"Successful": json_match_list}]
            if json_data: 
                message_list.append({"No match for name found": json_data})
            if json_skip_list: 
                message_list.append({"New name not unique": json_skip_list})
            return jsonify(*message_list), 201

    return jsonify({"message": "Client information changed successfully"}), 201

## View

@app.route("/api/clients", methods = ["GET"])
def view_clients():
    json = request.get_json()
    user = verify_user(json)
    if 'error' in user: 
        return jsonify(user), 401

    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        if "admin" in user:
            result_list = cursor.execute("SELECT * FROM clients").fetchall()
        else:
            result_list = user_info(user)
            if "error" in result_list:
                return jsonify(result_list), 404
            else:
                return {"id": result_list[0], "name": result_list[1]}

    if len(result_list) < 1: 
        return jsonify({"message": "Table empty"}), 404
    
    if not json:
        return jsonify(display_in_json(result_list, "clients"))
    
    return jsonify(view_handler(json, result_list, 'clients')), 200

#########################################################################
#########################################################################
## Transactions routes

## View

@app.route("/api/transactions", methods = ["GET"])
def view_transactions():
    json = request.get_json()
    user = verify_user(json)
    if 'error' in user: 
        return jsonify(user), 401

    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        if "admin" in user:
            result_list = cursor.execute("SELECT * FROM transactions").fetchall()
        else:
            result_list = [user_info(user)]
    if len(result_list) < 1: 
        return jsonify({"message": "Table empty"}), 404
    
    if not json:
        return jsonify(display_in_json(result_list, "transactions"))
    
    return jsonify(view_handler(json, result_list, 'transactions')), 200

## Main initialization

if __name__ == '__main__':
    init_db()
    app.run(debug = True)
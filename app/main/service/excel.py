import csv
import os
from collections import Iterable

from bson import ObjectId
from openpyxl import load_workbook
from app.db.connection import mongo

from app.db.Models.excel_template import ExcelTemplate


def generate_from_template(input_path, output_path, template):
    wb = load_workbook(input_path, read_only=True, data_only=True)

    keys = template.keys()
    data = []

    # WRITE VALUES METHOD
    def append_value(index, key, value):
        # CREATE ROW IF IT DOES NOT EXIST
        try:
            row = data[index]
        except IndexError as e:
            row = {k: None for k in keys}
            data.append(row)

        # INSERT VALUE
        row[key] = value
        return

    # READ TEMPLATE
    for key, sources in template.items():
        i = 0

        # CHECK IF SOURCE IS LIST
        # IF NOT TURN IT INTO ITERABLE
        if not isinstance(sources, list):
            sources = [sources]

        for source in sources:
            sheet = source['sheet']
            cell_range = source['range']
            cells = wb[sheet][cell_range]

            # CELL RANGE
            if isinstance(cells, Iterable):
                for row in cells:
                    for cell in row:
                        append_value(i, key, cell.value)
                        i += 1
            # SINGLE CELL
            else:
                cell = cells
                append_value(i, key, cell.value)
                i += 1

    # WRITE TO CSV
    if os.path.exists(output_path):
        os.remove(output_path)
    with open(output_path, 'w', newline='') as output_file:
        writer = csv.DictWriter(output_file, keys, delimiter=";")
        writer.writeheader()
        writer.writerows(data)

    return


def get_template_by_id(template_id):
    template = mongo.db.excel_template.find_one({"_id": ObjectId(template_id)})
    template['_id'] = str(template["_id"])
    return template


def get_templates_by_user(uid):
    template = mongo.db.excel_template.find({"user": uid}, {"_id": 1, "name": 1, "template": 1})
    res = []
    for temp in template:
        temp['_id'] = str(temp["_id"])
        res.append(temp)
    return res


def get_templates():
    template = mongo.db.excel_template.find({}, {"_id": 1, "name": 1, "template": 1})
    res = []
    for temp in template:
        temp['_id'] = str(temp["_id"])
        res.append(temp)
    return res


def create_template(data):
    mongo.db.excel_template.insert_one(data)
    return {"status": "CREATED", "code": 200}


def update_template(template_id, data):
    myquery = {"_id": ObjectId(template_id)}
    newvalues = {"$set": data}
    mongo.db.excel_template.update_one(myquery, newvalues)
    return {"status": "UPDATED", "tempalte_id": template_id, "code": 200}


def delete_template_by_id(template_id):
    query = {"_id": ObjectId(template_id)}
    mongo.db.excel_template.remove(query)
    return {"status": "DELETED", "tempalte_id": template_id, "code": 200}


def generate_csv_from_excel(inpath, outpath, template_id):
    tempalate = get_template_by_id(template_id)
    generate_from_template(inpath, outpath, tempalate["template"])
    return outpath

from pyexcelerate import Workbook, Color


def generate_xlsx(filepath, data, sheet_name='Data'):
    wb = Workbook()
    ws = wb.new_sheet(sheet_name, data=data)
    header_style = ws.get_row_style(1)
    header_style.fill.background = Color(240, 250, 240)
    header_style.font.bold = True
    wb.save(filepath)


def generate_csv(filepath, data):
    import csv
    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        writer.writerows(data)
        csvfile.close()
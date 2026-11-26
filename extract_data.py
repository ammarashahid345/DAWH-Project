import zipfile, xml.etree.ElementTree as ET, csv, re, os

src = "/mnt/c/Users/user1/OneDrive/Desktop/DAWH/Pharma.Supply.Chain.Dataset/Pharma_Supply_Chain_Source_Data.xlsx"
out = "data/extracted_full"
os.makedirs(out, exist_ok=True)

ns = {
    "m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "p": "http://schemas.openxmlformats.org/package/2006/relationships"
}

with zipfile.ZipFile(src) as z:
    wb = ET.fromstring(z.read("xl/workbook.xml"))
    rels = ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))
    targets = {x.attrib["Id"]: x.attrib["Target"] for x in rels}

    shared = []
    if "xl/sharedStrings.xml" in z.namelist():
        root = ET.fromstring(z.read("xl/sharedStrings.xml"))
        for si in root.findall("m:si", ns):
            shared.append("".join(t.text or "" for t in si.findall(".//m:t", ns)))

    wanted = {"05_pharmacy_sales_data"}

    for sheet in wb.findall("m:sheets/m:sheet", ns):
        name = sheet.attrib["name"].strip()
        if name not in wanted:
            continue

        target = targets[sheet.attrib["{"+ns["r"]+"}id"]]
        path = target.lstrip("/") if target.startswith("/") else "xl/" + target
        path = os.path.normpath(path)

        filename = os.path.join(out, name + ".csv")
        count = 0

        with z.open(path) as f, open(filename, "w", newline="", encoding="utf-8-sig") as csvfile:
            writer = csv.writer(csvfile)
            for event, row in ET.iterparse(f, events=("end",)):
                if row.tag.endswith("}row"):
                    values = []
                    for cell in row:
                        if not cell.tag.endswith("}c"):
                            continue
                        ref = cell.attrib.get("r", "")
                        col = re.match(r"[A-Z]+", ref).group()
                        col_num = 0
                        for ch in col:
                            col_num = col_num * 26 + ord(ch) - 64
                        while len(values) < col_num:
                            values.append("")
                        v = cell.find("m:v", ns)
                        inline = cell.find("m:is", ns)
                        value = ""
                        if v is not None and v.text is not None:
                            value = v.text
                            if cell.attrib.get("t") == "s":
                                value = shared[int(value)]
                        elif inline is not None:
                            value = "".join(t.text or "" for t in inline.findall(".//m:t", ns))
                        values[col_num - 1] = value
                    writer.writerow(values)
                    count += 1
                    row.clear()

        print(f"{name}: {count-1} data rows extracted -> {filename}")

import json, re, xml.etree.ElementTree as ET
from pathlib import Path

def strip_ns(content):
    content = re.sub(r'<(\w+:)', '<', content)
    content = re.sub(r'</(\w+:)', '</', content)
    content = re.sub(r'\s+xmlns(?::\w+)?="[^"]*"', '', content)
    content = re.sub(r'\s+\w+:\w+="[^"]*"', '', content)
    return content

def parse_coord_string(s):
    coords = []
    for part in s.strip().split():
        vals = part.split(',')
        if len(vals) >= 2:
            coords.append([float(vals[0]), float(vals[1])])
    return coords

def kml_to_geojson(kml_path):
    raw = Path(kml_path).read_text(encoding='utf-8', errors='replace')
    raw = strip_ns(raw)
    root = ET.fromstring(raw)
    features = []

    for pm in root.iter('Placemark'):
        name_el = pm.find('name')
        name = name_el.text if name_el is not None else ''
        props = {'name': name}

        for el in pm.iter('Point'):
            coord_el = el.find('coordinates')
            if coord_el is not None:
                c = parse_coord_string(coord_el.text)
                if c:
                    features.append({'type':'Feature','properties':props,'geometry':{'type':'Point','coordinates':c[0]}})

        for el in pm.iter('LineString'):
            coord_el = el.find('coordinates')
            if coord_el is not None:
                c = parse_coord_string(coord_el.text)
                if c:
                    features.append({'type':'Feature','properties':props,'geometry':{'type':'LineString','coordinates':c}})

        for el in pm.iter('Polygon'):
            outer = el.find('.//outerBoundaryIs//coordinates')
            if outer is not None:
                c = parse_coord_string(outer.text)
                if c:
                    features.append({'type':'Feature','properties':props,'geometry':{'type':'Polygon','coordinates':[c]}})

    return {'type':'FeatureCollection','features':features}

kmls = ['MicroAMVA_2016', 'Geomorfologia', 'Limite_urbano', 'Lineas_Sismicas']
for name in kmls:
    kml_path = Path(f'docs/{name}.kml')
    if kml_path.exists():
        geojson = kml_to_geojson(str(kml_path))
        out = Path(f'docs/{name}.geojson')
        out.write_text(json.dumps(geojson), encoding='utf-8')
        print(f'OK {name}.geojson — {len(geojson["features"])} features')
    else:
        print(f'FALTA: {kml_path}')

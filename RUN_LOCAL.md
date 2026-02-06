# Supertools PWA (Updated) — Run Local

## 1) Create venv + install
```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

pip install -r requirements.txt
```

## 2) Setup DB (SQLite by default)
```bash
flask --app app.py db upgrade
```

> ถ้าไม่มี migration บางเครื่อง ให้รัน `python app.py` ก่อน 1 ครั้ง เพื่อให้ `bootstrap()` ทำ `db.create_all()` และ seed ข้อมูลพื้นฐาน

## 3) Run
```bash
python app.py
```

Open: http://127.0.0.1:5000

## New pages added
- /equipment/categories/<id>/edit (แก้ไขหมวดหมู่)
- /equipment/statuses (ตั้งค่าสถานะอุปกรณ์)
- /import (นำเข้าข้อมูล CSV)
- /notifications (แจ้งเตือน)
- /reports/ar-aging (หน้าไม่พังแล้ว)

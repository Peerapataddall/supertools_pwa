/* =========================
   PWA: Service Worker
   ========================= */
if ("serviceWorker" in navigator) {
  window.addEventListener("load", async () => {
    try {
      const reg = await navigator.serviceWorker.register("/static/service-worker.js");
      console.log("[SW] registered", reg);

      // อัปเดตเวอร์ชันทันทีเมื่อมี SW ใหม่
      if (reg.waiting) reg.waiting.postMessage({ type: "SKIP_WAITING" });
      reg.addEventListener("updatefound", () => {
        const sw = reg.installing;
        sw?.addEventListener("statechange", () => {
          if (sw.state === "installed" && reg.waiting) {
            reg.waiting.postMessage({ type: "SKIP_WAITING" });
          }
        });
      });
      navigator.serviceWorker.addEventListener("controllerchange", () => location.reload());
    } catch (e) {
      console.warn("[SW] register fail", e);
    }
  });
}

/* =========================================================
   Autocomplete อุปกรณ์ (SKU + ชื่อ) ด้วย <datalist> กลาง
   - ใช้ event delegation -> แถวใหม่ใช้งานได้อัตโนมัติ
   - debounce แยกตาม input แต่ละตัว (WeakMap)
   - ต้องมี <datalist id="equip-options"></datalist> ในหน้า
   ========================================================= */
(function () {
  const dl = document.getElementById("equip-options");
  if (!dl) return; // ถ้าไม่มี datalist แสดงว่าไม่ใช่หน้านี้

  const timers = new WeakMap(); // เก็บ debounce ต่อ element

  function renderOptions(items) {
    dl.innerHTML = "";
    (items || []).forEach((it) => {
      const opt = document.createElement("option");
      // ค่าใน input เมื่อเลือก
      opt.value = `[${it.sku}] ${it.name}`;
      // label เผื่อเบราว์เซอร์โชว์แยก
      opt.label = it.label || `${it.sku} · ${it.name}`;
      dl.appendChild(opt);
    });
  }

  async function searchEquip(q) {
    if (!q || q.length < 1) {
      dl.innerHTML = "";
      return;
    }
    try {
      const res = await fetch(`/api/equipment/search?q=${encodeURIComponent(q)}&limit=20`);
      if (!res.ok) return;
      const data = await res.json();
      renderOptions(data);
    } catch (e) {
      dl.innerHTML = "";
    }
  }

  // พิมพ์ในช่องใด ๆ ที่มี .js-equip-input -> ยิงค้นหา (debounce 200ms)
  document.addEventListener("input", (ev) => {
    const el = ev.target;
    if (!el.classList || !el.classList.contains("js-equip-input")) return;

    const q = (el.value || "").trim();
    clearTimeout(timers.get(el));
    const t = setTimeout(() => searchEquip(q), 200);
    timers.set(el, t);
  });

  // โฟกัสแล้วโหลดคำแนะนำตามค่าปัจจุบัน
  document.addEventListener("focusin", (ev) => {
    const el = ev.target;
    if (!el.classList || !el.classList.contains("js-equip-input")) return;
    const q = (el.value || "").trim();
    searchEquip(q);
  });

  // กัน scroll wheel ไปเปลี่ยนค่าช่อง number โดยไม่ตั้งใจ (quality-of-life)
  document.addEventListener("wheel", (ev) => {
    const el = ev.target;
    if (el instanceof HTMLInputElement && el.type === "number" && el.matches(".item-qty, .item-dur, .item-price, .item-disc")) {
      el.blur(); // ต้อง blur ก่อนเพื่อไม่ให้เลื่อนแล้วค่าขยับ
    }
  }, { passive: true });
})();

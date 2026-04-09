from __future__ import annotations

import argparse


DEFAULT_DB = "carlist_type_mapping_python.db"
DEFAULT_BOOTSTRAP_XLSX = "../Carlist_Type_Mapping_RAW.xlsx"
DEFAULT_LIMIT = 100
DEFAULT_DELAY = 2.0
HTTP_BACKOFF_SECONDS = (0, 5, 15)
RENDER_WAIT_MS = 1000
WIKIPEDIA_SEARCH_LIMIT = 3
OFFICIAL_CANDIDATE_LIMIT = 5
SITEMAP_NESTED_LIMIT = 10
OFFICIAL_CRAWL_MAX_DEPTH = 2
OFFICIAL_CRAWL_MAX_PAGES = 10
OFFICIAL_CRAWL_MAX_LINKS_PER_PAGE = 120
WIKIPEDIA_SEARCH_API = "https://en.wikipedia.org/w/api.php"
WIKIPEDIA_SUMMARY_API = "https://en.wikipedia.org/api/rest_v1/page/summary/"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
FINAL_TYPES = {"diesel", "petrol", "electric", "electric/petrol"}
ALLOWED_TYPES = FINAL_TYPES | {"unknown"}
SITEMAP_PRIORITY_KEYWORDS = ["model", "vehicle", "car", "product", "lineup", "range"]

# Regex patterns applied to car_info (case-insensitive) to infer fuel type directly.
# Format: list of (compiled_pattern, fuel_type, evidence_url)
# Checked before any HTTP request, after BRAND_DEFAULTS and KEYWORDS check.
import re as _re
CAR_INFO_PATTERNS: list[tuple[object, str, str]] = [
    # Nissan Diesel / UD commercial trucks: CD/CW/CK/PW/BKR/BPR/BWR/AHR series
    (_re.compile(r"\bNISSAN\s+(CD|CW|CK|PW|BKR|BPR|BWR|AHR)\s*\d", _re.IGNORECASE),
     "diesel", "https://en.wikipedia.org/wiki/Nissan_Diesel"),

    # Peugeot passenger cars sold in Malaysia — all petrol (THP/PureTech 1.2/1.6T)
    # Models: 208, 2008, 308, 3008, 408, 5008, 508, 207, Traveller
    # Peugeot does not sell diesel passenger cars in Malaysia
    (_re.compile(r"\bPEUGEOT\s+(207|208|2008|308|3008|408|5008|508|TRAVELLER|LANDTREK)\b",
                 _re.IGNORECASE),
     "petrol", "https://en.wikipedia.org/wiki/Peugeot_3008"),

    # Isuzu passenger / lifestyle models — petrol (MU-X, D-Max petrol variant exists but rare)
    # Isuzu brand default is diesel, but MU-X has petrol option — handled via car_info keywords

    # Mitsubishi commercial trucks — diesel
    (_re.compile(r"\bMITSUBISHI\s+(CANTER|FUSO|FE|FG|FK|FM|FN|FP|FQ|FV|FZ|FIGHTER)\b",
                 _re.IGNORECASE),
     "diesel", "https://en.wikipedia.org/wiki/Mitsubishi_Fuso_Truck_and_Bus_Corporation"),


    # Toyota commercial trucks / Dyna / Hiace cargo — diesel
    (_re.compile(r"\bTOYOTA\s+(DYNA|HINO|TOYOACE)\b", _re.IGNORECASE),
     "diesel", "https://en.wikipedia.org/wiki/Toyota_Dyna"),

    # Mazda BT-50 pickup — diesel (in Malaysia market)
    (_re.compile(r"\bMAZDA\s+BT.?50\b", _re.IGNORECASE),
     "diesel", "https://en.wikipedia.org/wiki/Mazda_BT-50"),

    # Volvo trucks: FH/FM/FL/FMX/FE series — always diesel
    (_re.compile(r"\bVOLVO\s+(FH|FM|FL|FMX|FE|NH|NL)\d*\b", _re.IGNORECASE),
     "diesel", "https://www.volvotrucks.com/en-en/trucks.html"),
    # VOLVO PRIME MOVER / REBUILD / REBUILT — heavy truck, diesel
    (_re.compile(r"\bVOLVO\s+(PRIME\s+MOVER|REBUILD|REBUILT)\b", _re.IGNORECASE),
     "diesel", "https://www.volvotrucks.com/en-en/trucks.html"),

    # Volvo Cars T8 / RECHARGE — plug-in hybrid (electric/petrol)
    # T8 across all Volvo car models is the PHEV powertrain
    (_re.compile(r"\bVOLVO\s+\S+\s+.*T8\b", _re.IGNORECASE),
     "electric/petrol", "https://en.wikipedia.org/wiki/Volvo_Recharge"),
    (_re.compile(r"\bVOLVO\s+\S+T8\b", _re.IGNORECASE),
     "electric/petrol", "https://en.wikipedia.org/wiki/Volvo_Recharge"),
    (_re.compile(r"\bVOLVO\s+.*RECHARGE\b", _re.IGNORECASE),
     "electric/petrol", "https://en.wikipedia.org/wiki/Volvo_Recharge"),

    # Volvo C40 / EX40 / EX90 — pure electric (also handles typos like C40P8)
    (_re.compile(r"\bVOLVO\s+(C40|EX40|EX90)", _re.IGNORECASE),
     "electric", "https://en.wikipedia.org/wiki/Volvo_Cars#Electric_vehicles"),

    # Volvo Cars T2/T3/T4/T5/T6 (non-T8) — petrol
    # S60/S80/S90/V40/V60/V90/XC40/XC60/XC90 with T2-T6 are petrol turbocharged
    (_re.compile(r"\bVOLVO\s+(S60|S80|S90|V40|V60|V90|XC40|XC60|XC90|C30|C70)\b",
                 _re.IGNORECASE),
     "petrol", "https://en.wikipedia.org/wiki/Volvo_Cars"),

    # ── Toyota Industrial Equipment ──────────────────────────────────────────
    # Toyota FD / 7FD / 8FD series forklifts — diesel (FD = diesel drive)
    (_re.compile(r"\bTOYOTA\b.*\b\d*FD[A-Z]?\d+\b", _re.IGNORECASE),
     "diesel", "https://www.toyota-industries.com/products/forklift/"),
    # Toyota FB / FBR / FBRE series forklifts — battery electric
    (_re.compile(r"\bTOYOTA\b.*\b\d*FB[A-Z]*\d+\b", _re.IGNORECASE),
     "electric", "https://www.toyota-industries.com/products/forklift/"),
    # Toyota SD25 / 3SD / 4SD shovel loaders — diesel
    (_re.compile(r"\bTOYOTA\s+\d*SD\d+\b", _re.IGNORECASE),
     "diesel", "https://en.wikipedia.org/wiki/Toyota"),

    # ── Toyota commercial truck chassis codes — diesel ───────────────────────
    # KDY/KDH = HiAce diesel, LY = Light Truck diesel, XZU/XZY = Dyna/Toyoace diesel
    # BU = older diesel truck; KDY280/KDY231 etc all K-series diesel
    (_re.compile(
        r"\bTOYOTA\s+(?:REBUILD\s+)?(?:KDY|KDH|LY|XZU|XZY|BU)\d+\b",
        _re.IGNORECASE),
     "diesel", "https://en.wikipedia.org/wiki/Toyota_HiAce"),

    # ── Toyota passenger/lifestyle models — petrol only in Malaysia ──────────
    # Vios, Corolla Altis (no diesel/hybrid variant sold here)
    (_re.compile(r"\bTOYOTA\s+(?:VIOS|ALTIS)\b", _re.IGNORECASE),
     "petrol", "https://en.wikipedia.org/wiki/Toyota_Vios"),
    # GR86 / 86 — rear-wheel-drive sports coupe, petrol only
    (_re.compile(r"\bTOYOTA\s+(?:GR\s*)?86\b", _re.IGNORECASE),
     "petrol", "https://en.wikipedia.org/wiki/Toyota_GR86"),
    # Hiace passenger (KDH = diesel already caught above; HIACE alone → diesel)
    (_re.compile(r"\bTOYOTA\s+(?:HIACE|HI-ACE)\b", _re.IGNORECASE),
     "diesel", "https://en.wikipedia.org/wiki/Toyota_HiAce"),
    # Land Cruiser / Fortuner / HiLux → diesel in Malaysia market
    (_re.compile(r"\bTOYOTA\s+(?:LAND\s*CRUISER|LANDCRUISER|FORTUNER|HILUX)\b",
                 _re.IGNORECASE),
     "diesel", "https://en.wikipedia.org/wiki/Toyota_Land_Cruiser"),

    # ── Nissan commercial truck chassis codes — diesel ───────────────────────
    # CG4/CG5 prime movers (not covered by existing CD/CW pattern)
    (_re.compile(r"\bNISSAN\s+CG[4-6]\b", _re.IGNORECASE),
     "diesel", "https://en.wikipedia.org/wiki/Nissan_Diesel"),
    # GK4/GW4/GKB4 prime movers, MK/SK/PK Condor series — all diesel
    (_re.compile(
        r"\bNISSAN\s+(?:CONDOR\s+)?(?:GK[4-9]|GW[4-9]|GKB\d|MK3[5-9]|MK4\d|SK\d{2}|PK\d{2})\b",
        _re.IGNORECASE),
     "diesel", "https://en.wikipedia.org/wiki/Nissan_Diesel"),
    # Urvan/Caravan/Civilian — diesel vans
    (_re.compile(r"\bNISSAN\s+(?:URVAN|CARAVAN|CIVILIAN)\b", _re.IGNORECASE),
     "diesel", "https://en.wikipedia.org/wiki/Nissan_Caravan"),

    # ── Nissan passenger models — petrol in Malaysia ─────────────────────────
    # Almera / Livina / Latio — petrol only (no diesel variant in MY)
    (_re.compile(r"\bNISSAN\s+(?:ALMERA|ALMEERA|GRAND\s+LIVINA|LIVINA|LATIO)\b",
                 _re.IGNORECASE),
     "petrol", "https://en.wikipedia.org/wiki/Nissan_Almera"),
    # X-Trail without hybrid keyword → petrol (2.0L/2.5L petrol CVT in Malaysia)
    (_re.compile(r"\bNISSAN\s+X[-\s]?TRAIL\b(?!.*\b(?:HYBRID|E-POWER|EPOWER)\b)",
                 _re.IGNORECASE),
     "petrol", "https://en.wikipedia.org/wiki/Nissan_X-Trail"),
    # GT-R — twin-turbo V6 petrol sports car
    (_re.compile(r"\bNISSAN\s+GT-?R\b", _re.IGNORECASE),
     "petrol", "https://en.wikipedia.org/wiki/Nissan_GT-R"),

    # ── Honda passenger models — petrol in Malaysia ──────────────────────────
    # BR-V / WR-V / Brio — no hybrid variant sold in Malaysia
    (_re.compile(r"\bHONDA\s+(?:BRV|BR-V|WRV|WR-V|BRIO)\b", _re.IGNORECASE),
     "petrol", "https://en.wikipedia.org/wiki/Honda_BR-V"),
    # City / Civic / Jazz / HR-V / CR-V without hybrid keyword → petrol
    (_re.compile(
        r"\bHONDA\s+(?:CITY|CIVIC|JAZZ|HRV|HR-V|CRV|CR-V|ACCORD|ODYSSEY|STEPWGN|STEPWAGON)\b"
        r"(?!.*\b(?:HYBRID|E-HEV|PHEV|PLUG.IN)\b)",
        _re.IGNORECASE),
     "petrol", "https://en.wikipedia.org/wiki/Honda_City"),
]

# Brands whose fuel type is unambiguous.
# Each entry: brand → (fuel_type, evidence_url)
# evidence_url is a stable public page that confirms the powertrain — no HTTP request made.
BRAND_DEFAULTS: dict[str, tuple[str, str]] = {
    # All-electric manufacturers
    "TESLA":       ("electric", "https://www.tesla.com/models"),
    "NETA":        ("electric", "https://www.neta-auto.com/en/"),
    "XPENG":       ("electric", "https://www.xpeng.com/en/"),
    "LEAPMOTOR":   ("electric", "https://www.leapmotor.com/en/"),
    "SMART":       ("electric", "https://www.smart.com/en/"),
    # Commercial trucks / buses — diesel only in this market
    "HINO":        ("diesel",   "https://www.hino-global.com/products/"),
    "UD":          ("diesel",   "https://www.udtrucks.com/products/"),
    "ISUZU":       ("diesel",   "https://www.isuzu.com/en/lineup/"),
    "SCANIA":      ("diesel",   "https://www.scania.com/group/en/home/products-and-services/trucks.html"),
    "MAN":         ("diesel",   "https://www.man.eu/en/trucks/"),
    "SINOTRUK":    ("diesel",   "https://www.sinotruk.com/pro/"),
    "FOTON":       ("diesel",   "https://www.foton.com/product/truck/"),
    "HOHAN":       ("diesel",   "https://www.sinotruk.com/pro/"),
    "SHACMAN":     ("diesel",   "https://www.shacman.com/products/"),
    "CAMC":        ("diesel",   "https://en.camc.com.cn/product/"),
    "AUMAN":       ("diesel",   "https://www.foton.com/product/truck/"),
    "HONGYAN":     ("diesel",   "https://en.wikipedia.org/wiki/SAIC-IVECO-Hongyan"),
    "JMC":         ("diesel",   "https://en.wikipedia.org/wiki/Jiangling_Motors"),
    "DONGFENG":    ("diesel",   "https://www.dongfeng-global.com/products/"),
    "DONG":        ("diesel",   "https://www.dongfeng-global.com/products/"),
    "FAW":         ("diesel",   "https://en.wikipedia.org/wiki/FAW_Group"),
    "CAM":         ("diesel",   "https://en.wikipedia.org/wiki/King_Long"),
    "CHA":         ("diesel",   "https://en.wikipedia.org/wiki/Changan_Automobile"),
    "TATA":        ("diesel",   "https://www.tatamotors.com/products/commercial-vehicles/"),
    "INOKOM":      ("diesel",   "https://en.wikipedia.org/wiki/Inokom"),
    "EUROSTAR":    ("diesel",   "https://en.wikipedia.org/wiki/Eurostar_(truck)"),
    "XIAMEN":      ("diesel",   "https://en.wikipedia.org/wiki/Xiamen_King_Long"),
    "KING":        ("diesel",   "https://en.wikipedia.org/wiki/King_Long"),
    "OTHERS":      ("unknown",  ""),   # Semi-trailers — no engine
    # Construction & agricultural equipment — always diesel
    "HITACHI":     ("diesel",   "https://www.hitachicm.com/global/en/products/"),
    "KOMATSU":     ("diesel",   "https://www.komatsu.com/en/products/"),
    "KUBOTA":      ("diesel",   "https://www.kubota.com/products/"),
    "KOBELCO":     ("diesel",   "https://www.kobelco-cranes.com/products/"),
    "SUMITOMO":    ("diesel",   "https://en.wikipedia.org/wiki/Sumitomo_Heavy_Industries"),
    "CATERPILLAR": ("diesel",   "https://www.cat.com/en_US/products/new/equipment.html"),
    "LIUGONG":     ("diesel",   "https://www.liugong.com/en/products/"),
    "LONKING":     ("diesel",   "https://en.wikipedia.org/wiki/Lonking"),
    "JCB":         ("diesel",   "https://www.jcb.com/en-gb/products"),
    "CASE":        ("diesel",   "https://www.casece.com/en-au/products"),
    "BOBCAT":      ("diesel",   "https://www.bobcat.com/en/equipment"),
    "KATO":        ("diesel",   "https://en.wikipedia.org/wiki/Kato_Works"),
    "TADANO":      ("diesel",   "https://www.tadano.com/products/"),
    "SAKAI":       ("diesel",   "https://en.wikipedia.org/wiki/Sakai_Heavy_Industries"),
    "HAMM":        ("diesel",   "https://www.wirtgen-group.com/en-gb/hamm/products/"),
    "NEW_HOLLAND": ("diesel",   "https://www.newholland.com/apac/en-au/products/"),
    "LANDINI":     ("diesel",   "https://www.landini.it/en/products/"),
    "YANMAR":      ("diesel",   "https://www.yanmar.com/global/agri/products/"),
    "DEUTZ":       ("diesel",   "https://www.deutz.com/en/products/"),
    "HELI":        ("diesel",   "https://en.wikipedia.org/wiki/Heli_(forklift)"),
    "HANGCHA":     ("diesel",   "https://en.wikipedia.org/wiki/Hangcha_Group"),
    "TCM":         ("diesel",   "https://en.wikipedia.org/wiki/TCM_Corporation"),
    "UNICARRIERS": ("diesel",   "https://en.wikipedia.org/wiki/UniCarriers"),
    # Malaysian national car brands — all petrol (no diesel/EV variants sold locally)
    "PERODUA":     ("petrol",   "https://www.perodua.com.my/cars.html"),
    # Motorcycles — petrol
    "HARLEY":      ("petrol",   "https://www.harley-davidson.com/us/en/motorcycles.html"),
    "DUCATI":      ("petrol",   "https://www.ducati.com/ww/en/bikes"),
    "TRIUMPH":     ("petrol",   "https://www.triumphmotorcycles.com/motorcycles"),
    "YAMAHA":      ("petrol",   "https://www.yamaha-motor.com/world/product/motorcycle/"),
}
BRAND_URL_PATTERNS: dict[str, list[str]] = {
    "TOYOTA":        ["https://www.toyota.com/{model}", "https://www.toyota.com/models/{model}"],
    "BMW":           ["https://www.bmw.com/en/all-models/{model}.html"],
    "AUDI":          ["https://www.audi.com/en/models/{model}.html"],
    "HONDA":         ["https://www.honda.com.my/cars/{model}", "https://automobiles.honda.com/{model}"],
    "NISSAN":        ["https://www.nissan-global.com/EN/VEHICLE/{model}/"],
    "MAZDA":         ["https://www.mazda.com/en/vehicles/{model}/"],
    "MITSUBISHI":    ["https://www.mitsubishi-motors.com/en/vehicles/{model}/"],
    "MERCEDES_BENZ": ["https://www.mercedes-benz.com/en/vehicles/passenger-cars/{model}/"],
    "LAND_ROVER":    ["https://www.landrover.com/vehicles/{model}/index.html"],
    "LEXUS":         ["https://www.lexus.com/models/{model}"],
    "HYUNDAI":       ["https://www.hyundai.com/worldwide/en/cars/{model}"],
    "FORD":          ["https://www.ford.com/cars/{model}/"],
    "VOLVO":         ["https://www.volvocars.com/en/{model}/"],
    "PEUGEOT":       ["https://www.peugeot.com/en/all-cars/{model}.html"],
    "FERRARI":       ["https://www.ferrari.com/en-EN/auto/ferrari-{model}",
                      "https://www.ferrari.com/en-EN/auto/{model}"],
    "PORSCHE":       ["https://www.porsche.com/international/models/{model}/"],
    "VOLKSWAGEN":    ["https://www.volkswagen-newsroom.com/en/models/{model}"],
    "KIA":           ["https://www.kia.com/worldwide/vehicles/{model}.html"],
    "SUBARU":        ["https://www.subaru.com/vehicles/{model}/index.html"],
    "JAGUAR":        ["https://www.jaguar.com/jaguar-range/{model}/index.html"],
    "SKODA":         ["https://www.skoda-auto.com/models/{model}"],
    "RENAULT":       ["https://www.renault.com/en/vehicles/{model}.html"],
}
OFFICIAL_DOMAINS = {
    "AUDI": ["www.audi.com", "www.audiusa.com"],
    "BMW": ["www.bmw.com"],
    "TOYOTA": ["www.toyota.com", "global.toyota"],
    "HONDA": ["www.honda.com.my", "global.honda", "automobiles.honda.com"],
    "NISSAN": ["www.nissan.com.my", "nissan.com.my", "www.nissanusa.com"],
    "MAZDA": ["www.mazda.com", "www.mazdausa.com"],
    "MITSUBISHI": ["www.mitsubishi-motors.com", "www.mitsubishicars.com"],
    "MERCEDES_BENZ": ["www.mercedes-benz.com", "www.mbusa.com"],
    "LAND_ROVER": ["www.landrover.com", "www.landroverusa.com"],
    "LEXUS": ["www.lexus.com", "global.toyota"],
    "HYUNDAI": ["www.hyundai.com", "www.hyundaiusa.com"],
    "FORD": ["www.ford.com", "media.ford.com"],
    "MINI": ["www.mini.com", "www.miniusa.com"],
    "VOLVO": ["www.volvocars.com"],
    "PEUGEOT": ["www.peugeot.com.my"],  # peugeot.com returns 403 for bots
    "ISUZU": ["www.isuzu.co.jp", "www.isuzu.com"],
    "HINO": ["www.hino-global.com"],
    "UD": ["www.udtrucks.com"],
    "FERRARI": ["www.ferrari.com"],
    "PORSCHE": ["www.porsche.com"],
    "VOLKSWAGEN": ["www.volkswagen-newsroom.com"],
    "KIA": ["www.kia.com"],
    "SUBARU": ["www.subaru.com"],
    "JAGUAR": ["www.jaguar.com"],
    "SKODA": ["www.skoda-auto.com"],
    "RENAULT": ["www.renault.com"],
    "FAW": ["www.faw.com"],
    "MASERATI": ["www.maserati.com"],
    "LAMBORGHINI": ["www.lamborghini.com"],
    "JEEP": ["www.jeep.com"],
    "CHEVROLET": ["www.chevrolet.com"],
    "CITROEN": ["www.citroen.com"],
    # Malaysian-market brands
    "PERODUA": ["www.perodua.com.my"],
    "PROTON": ["www.proton.com"],
    "NAZA": ["www.naza.com.my"],
    # Daihatsu (parent of Perodua)
    "DAIHATSU": ["www.daihatsu.co.jp"],
    # Range Rover is a nameplate of Land Rover — share the same domain
    "RANGE_ROVER": ["www.landrover.com"],
    # European brands
    "ALFA_ROMEO": ["www.alfaromeo.com"],
    "ASTON_MARTIN": ["www.astonmartin.com"],
    "SUZUKI": ["www.globalsuzuki.com", "www.suzuki.co.jp"],
    "FIAT": ["www.fiat.com"],
    "SEAT": ["www.seat.com"],
    # Chinese brands (mixed/EV market)
    "BYD": ["www.byd.com"],
    "MG": ["www.mgmotor.co.uk", "www.mg.com"],
    "GWM": ["www.gwm-global.com"],
    "GREAT_WALL": ["www.gwm-global.com"],
    "HAVAL": ["www.haval.com"],
    "CHERY": ["www.cheryinternational.com"],
    "JAC": ["www.jac-global.com"],
    "MAXUS": ["www.maxus-ev.com"],
    # Other Asian brands
    "TATA": ["www.tatamotors.com"],
    "INOKOM": ["www.inokom.com.my"],
    "INFINITI": ["www.infiniti.com"],
    "BENTLEY": ["www.bentleymotors.com"],
    "MCLAREN": ["www.mclaren.com"],
    "ROLLS_ROYCE": ["www.rolls-roycemotorcars.com"],
    "ROLLS": ["www.rolls-roycemotorcars.com"],
    "LOTUS": ["www.lotuscars.com"],
    "MASERATI": ["www.maserati.com"],
    "LAMBORGHINI": ["www.lamborghini.com"],
    "ASTON_MARTIN": ["www.astonmartin.com"],
    "ALFA_ROMEO": ["www.alfaromeo.com"],
    "CHRYSLER": ["www.chrysler.com"],
    "DODGE": ["www.dodge.com"],
    "HUMMER": ["www.gmc.com"],   # GMC now sells Hummer EV
    "INFINITI": ["www.infiniti.com"],
    "SUZUKI": ["www.globalsuzuki.com"],
    "MAN": ["www.man.eu"],        # MAN Truck & Bus
    "SHACMAN": ["www.shacman.com"],
    "DFSK": ["www.dfsk.com"],
    "CHANGAN": ["www.changan.com.cn"],
    "GAC": ["www.gac-motor.com"],
}
KEYWORDS = {
    "electric/petrol": [
        "hybrid",
        "plug-in hybrid",
        "plug in hybrid",
        "phev",
        "mhev",
        "petrol hybrid",
        "gasoline hybrid",
    ],
    "electric": [
        "battery electric",
        "fully electric",
        "all-electric",
        "all electric",
        "electric vehicle",
        "electric car",
        "electric suv",
        "bev",
    ],
    "diesel": [
        "diesel",
        "turbodiesel",
        "bluehdi",
        "tdi",
        "dci",
        "crdi",
        "duratorq",
        "common-rail diesel",
    ],
    "petrol": [
        "petrol",
        "gasoline",
        "gasoline engine",
        "petrol engine",
        "tsi",
        "tfsi",
        "ecoboost",
        "turbo petrol",
        "naturally aspirated",
        "v8 engine",
        "v12 engine",
        "v6 engine",
        "flat-6",
        "boxer engine",
        "internal combustion",
        "otto cycle",
        "fuel injected",
        "direct injection petrol",
        "gdi",
        # Peugeot/Citroen petrol engine codes
        "puretech",
        "thp",
        "vti",
        "e-thp",
        # Renault/Nissan petrol codes
        "tce",
        "sce",
        # Hyundai/Kia petrol codes
        "gdi",
        "t-gdi",
        # Mitsubishi petrol
        "mivec",
        # Toyota petrol
        "vvt-i",
        "vvti",
        "d-4st",
        # Honda petrol
        "vtec",
        "i-vtec",
        # General turbo petrol markers
        "turbo petrol",
        "turbocharged petrol",
        "turbocharged gasoline",
    ],
}
NEGATIVE_HINTS = {
    "electric": ["diesel", "gasoline", "petrol", "hybrid"],
    "diesel": ["electric", "hybrid"],
    "petrol": ["electric", "hybrid", "diesel"],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fill Type and Evidence URL in a SQLite database using free public data sources."
    )
    parser.add_argument("--db", default=DEFAULT_DB, help="Path to the SQLite database.")
    parser.add_argument(
        "--bootstrap-xlsx",
        default=DEFAULT_BOOTSTRAP_XLSX,
        help="Excel file used only to initialize or rebuild the SQLite database.",
    )
    parser.add_argument(
        "--reimport-db",
        action="store_true",
        help="Rebuild the SQLite database from --bootstrap-xlsx before processing.",
    )
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Rows per run.")
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help="Delay in seconds between outbound HTTP requests.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-process rows that already have Type, Evidence URL, and URL Prove.",
    )
    parser.add_argument(
        "--allow-unknown",
        action="store_true",
        help="Also process rows where Type is 'unknown'. By default only empty Type rows are processed.",
    )
    parser.add_argument(
        "--browser",
        action="store_true",
        help="Use a headless browser (Playwright) to render JS-heavy model pages before extracting text.",
    )
    parser.add_argument(
        "--fix-hybrid",
        action="store_true",
        help="Re-classify rows where Type=electric/petrol but car_info contains no hybrid keyword (likely misclassified).",
    )
    return parser.parse_args()

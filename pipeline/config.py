"""
Pipeline constants — all hardcoded values live here.
"""
from pathlib import Path

# ---------------------------------------------------------------------------
# Directories
# ---------------------------------------------------------------------------
ROOT_DIR = Path(__file__).parent.parent
DATA_DIR = ROOT_DIR / "data"
OUTPUT_DIR = ROOT_DIR / "output"

# ---------------------------------------------------------------------------
# Account setup
# ---------------------------------------------------------------------------
ACCOUNTS_TEMPLATE_SHEET = "Template"
ACCOUNTS_TEMPLATE_SKIPROWS = 4
ACCOUNTS_VALID_ID_MIN = 10000

# ---------------------------------------------------------------------------
# Prospect IDs
# ---------------------------------------------------------------------------
PROSPECT_ID_PREFIX = "Z"   # ZoomInfo-style source uses Z0001–Z9999
SOURCE_LABEL = "Source 1"

# ---------------------------------------------------------------------------
# Validation thresholds
# ---------------------------------------------------------------------------
EMAIL_REGEX = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
FUZZY_EMAIL_THRESHOLD = 75
FUZZY_DOMAIN_THRESHOLD = 50

PRIVATE_DOMAINS: frozenset[str] = frozenset({
    "gmail.com", "yahoo.com", "yahoo.de", "hotmail.com", "aol.com",
    "outlook.com", "icloud.com", "mail.com", "live.com",
    "t-online.de", "web.de", "gmx.de", "info.com",
})

LEGAL_SUFFIXES: frozenset[str] = frozenset({
    # English
    "inc", "llc", "ltd", "corporation", "corp", "company", "co", "and",
    # German
    "gmbh", "ag", "kg", "ohg", "gbr", "ug", "eg", "ev", "se", "kgaa",
    "betrieb", "unternehmen", "firma", "holding", "gruppe", "group",
    # Swiss
    "sa", "sarl", "kollektivgesellschaft", "kommanditgesellschaft",
    "einzelfirma", "einzelunternehmen", "genossenschaft", "verein", "stiftung",
    "societe", "anonyme", "responsabilite", "limitee", "simple",
    "commandite", "cooperative", "fondation", "association",
    "societa", "anonima", "responsabilita", "limitata", "semplice",
    "accomandita", "cooperativa", "fondazione", "associazione",
    # Austrian
    "og", "reg", "gen", "privatstiftung", "gemeinnuetzige", "sparkasse",
    "bank", "versicherung", "aktiengesellschaft", "gesellschaft",
    "beschraenkter", "haftung", "offene", "handelsgesellschaft",
    "eingetragene", "mbh", "compagnie", "beteiligungsgesellschaft",
    "verwaltungs", "beteiligungs", "investment", "capital", "venture",
    "partners",
    # Polish
    "sp", "zoo", "sa", "spka", "jawna", "komandytowa", "partnerska",
    # Czech
    "sro", "as", "ks", "vos", "komanditni", "verejne", "obchodni", "spolecnost",
    # Slovak
    "doo", "akciova", "spolocnost", "komanditna",
    # Hungarian
    "kft", "rt", "nyrt", "zrt", "bt", "kkt", "felelossegu", "tarsasag",
    # Slovenian
    "dd", "kd",
    # Croatian / Serbian / Bosnian
    "jdoo", "jdd",
    # Bulgarian
    "ood", "eood",
    # Romanian
    "srl", "societate", "responsabilitate",
    # Baltic
    "ou", "sia", "uab", "ab",
    # CIS / Russian
    "ooo", "zao", "oao", "cjsc", "ojsc", "pao", "npo", "gup", "fgup",
    "mp", "ip", "chp", "pvt", "too", "ao", "mmc", "mchj", "ak",
    # Generic
    "the", "von", "zu",
})

# ---------------------------------------------------------------------------
# Deliverable export
# ---------------------------------------------------------------------------
SHEET_PASSWORD = "prospecting"

ERROR_CATEGORY_OPTIONS = [
    "Bad Name", "Bad Email", "Gender", "Duplicate", "Bad Domain",
    "No longer with the company", "Private Email", "Bad Country",
]

GENDER_OPTIONS = ["Male", "Female", "Unknown"]

COUNTRY_OPTIONS = [
    "AL - Albania", "AM - Armenia", "AT - Austria", "AZ - Azerbaijan",
    "BA - Bosnia/Herzeg.", "BG - Bulgaria", "HR - Croatia", "CZ - Czech Republic",
    "GE - Georgia", "HU - Hungary", "KZ - Kazakhstan", "KG - Kyrgyzstan",
    "MK - Macedonia", "MD - Moldavia", "ME - Montenegro", "PL - Poland",
    "RO - Romania", "RU - Russia", "RS - Serbia", "SK - Slovakia",
    "SI - Slovenia", "TJ - Tajikistan", "TM - Turkmenistan", "UA - Ukraine",
    "UZ - Uzbekistan", "DE - Germany", "LI - Liechtenstein", "CH - Switzerland",
    "BE - Belgium", "LU - Luxembourg", "NL - Netherlands", "FR - France",
    "PF - Frenc.Polynesia", "GP - Guadeloupe", "MQ - Martinique", "YT - Mayotte",
    "MC - Monaco", "NC - New Caledonia", "RE - Reunion", "IT - Italy",
    "SM - San Marino", "VA - Vatican City", "AF - Afghanistan", "DZ - Algeria",
    "BH - Bahrain", "BJ - Benin", "BF - Burkina Faso", "CM - Cameroon",
    "TD - Chad", "CD - Dem. Rep. Congo", "DJ - Djibouti", "EG - Egypt",
    "GA - Gabon", "GN - Guinea", "IQ - Iraq", "CI - Ivory coast",
    "KW - Kuwait", "ML - Mali", "MR - Mauretania", "MA - Morocco",
    "NE - Niger", "PK - Pakistan", "CG - Rep. of Congo", "SA - Saudi Arabia",
    "SN - Senegal", "TG - Togo", "TN - Tunisia", "AO - Angola",
    "BW - Botswana", "BI - Burundi", "CV - Cabo Verde", "GQ - Equatorial Guinea",
    "ER - Eritrea", "SZ - Eswatini", "ET - Ethiopia", "GM - Gambia",
    "GH - Ghana", "JO - Jordan", "KE - Kenya", "LB - Lebanon",
    "LS - Lesotho", "LR - Liberia", "LY - Libya", "MG - Madagascar",
    "MW - Malawi", "MU - Mauritius", "MZ - Mozambique", "NG - Nigeria",
    "OM - Oman", "PS - Palestine Reg.", "QA - Qatar", "RW - Rwanda",
    "ST - S.Tome,Principe", "SC - Seychelles", "SL - Sierra Leone",
    "ZA - South Africa", "SS - South Sudan", "SD - Sudan", "TZ - Tanzania",
    "UG - Uganda", "AE - Unit.Arab Emir.", "YE - Yemen", "ZM - Zambia",
    "ZW - Zimbabwe", "DK - Denmark", "EE - Estonia", "FO - Faroe Islands",
    "FI - Finland", "GL - Greenland", "IS - Iceland", "LV - Latvia",
    "LT - Lithuania", "NO - Norway", "SE - Sweden", "AD - Andorra",
    "CY - Cyprus", "GI - Gibraltar", "GR - Greece", "IL - Israel",
    "MT - Malta", "PT - Portugal", "ES - Spain", "TR - Turkey",
    "GG - Guernsey", "IE - Ireland", "IM - Isle of Man", "JE - Jersey",
    "AU - Australia", "CK - Cook Islands", "FJ - Fiji", "FM - Micronesia",
    "NZ - New Zealand", "PG - Pap. New Guinea", "SB - Solomon Islands",
    "TO - Tonga", "VU - Vanuatu", "CN - China", "HK - Hong Kong",
    "MO - Macao", "MN - Mongolia", "TW - Taiwan", "BD - Bangladesh",
    "IN - India", "LK - Sri Lanka", "JP - Japan", "KR - South Korea",
    "BT - Bhutan", "BN - Brunei Daruss.", "KH - Cambodia", "ID - Indonesia",
    "LA - Laos", "MY - Malaysia", "MV - Maldives", "MM - Myanmar",
    "NP - Nepal", "PH - Philippines", "SG - Singapore", "TH - Thailand",
    "TP - Timor-Leste", "VN - Vietnam", "AI - Anguilla", "AG - Antigua/Barbuda",
    "AR - Argentina", "AW - Aruba", "BS - Bahamas", "BB - Barbados",
    "BZ - Belize", "BM - Bermuda", "BO - Bolivia", "BQ - Bonaire, Saba",
    "BR - Brazil", "VG - Brit.Virgin Is.", "KY - Cayman Islands", "CL - Chile",
    "CO - Colombia", "CR - Costa Rica", "CW - Curacao", "DM - Dominica",
    "DO - Dominican Rep.", "EC - Ecuador", "SV - El Salvador", "GD - Grenada",
    "GT - Guatemala", "GY - Guyana", "HT - Haiti", "HN - Honduras",
    "JM - Jamaica", "MX - Mexico", "NI - Nicaragua", "PA - Panama",
    "PY - Paraguay", "PE - Peru", "PR - Puerto Rico", "SX - Sint Maarten",
    "KN - St Kitts&Nevis", "LC - St. Lucia", "VC - St. Vincent",
    "SR - Suriname", "TT - Trinidad,Tobago", "TC - Turksh Caicosin",
    "UY - Uruguay", "VE - Venezuela", "CA - Canada", "GU - Guam",
    "US - United States",
]

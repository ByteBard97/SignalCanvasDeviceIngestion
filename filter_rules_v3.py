"""Device universe filtering rules for pro AV selection.

Distinction: KEEP pro AV infrastructure/signal chain devices.
SKIP passive consumer accessories and non-AV gear.
"""

# Categories to keep
PATCHIFY_KEEP_CATS = {
    'audio', 'converter', 'network', 'camera', 'switcher',
    'controller', 'lighting', 'media-server', 'video-router', 'led-processor',
    'monitor',  # filtered further by model name
}

ES_KEEP_TYPES = {
    'switcher', 'speaker', 'audio-interface', 'amplifier', 'touch-screen',
    'audio-mixer', 'streaming-encoder', 'fiber-transmitter', 'control-processor',
    'converter', 'audio-dsp', 'router', 'network-switch', 'camera',
    'projector', 'media-player', 'lighting-console', 'recorder', 'ptz-camera',
    'kvm-extender', 'stage-box', 'hdbaset-extender', 'power-distribution',
    'intercom', 'wireless-microphone', 'wireless-receiver', 'wireless-transmitter',
    'di-box', 'splitter', 'patchbay',
}

# Manufacturers to skip entirely
SKIP_MFGS = {'', 'unknown', 'generic', 'industries', 'devices'}

# Consumer/non-AV patterns to skip
CONSUMER_SKIP = [
    r'\bmacbook\b', r'\biphone\b', r'\bipad\b', r'\bairpods\b', r'\bapple watch\b',
    r'\bapple tv\b', r'\bappletv\b', r'\bimac\b', r'\bmac mini\b', r'\bmac pro\b',
    r'\bgalaxy s\d+\b', r'\bpixel\b', r'\bplaystation\b', r'\bxbox\b', r'\bnintendo\b',
    r'\bsteam deck\b', r'\bgo pro\b', r'\bgopro\b', r'\bsjcam\b', r'\bdji\b',
    r'\bintel nuc\b', r'\bnuc\d', r'\brasberry\b', r'\barduino\b', r'\besp32\b',
    r'\bshelly\b', r'\bphilips hue\b', r'\bhue\b', r'\bnest\b', r'\bring\b',
    r'\bsmart plug\b', r'\bsmart switch\b', r'\bsmart home\b',
    r'\bconsumer\b', r'\bgaming\b', r'\bgamer\b',
    r'\bcharger\b', r'\bcharging\b', r'\bbattery\b', r'\bpower bank\b',
    r'\bkeyboard\b', r'\bmouse\b', r'\btrackpad\b', r'\bwebcam\b',
    r'\bbluetooth speaker\b', r'\bportable speaker\b',
    r'\bwireless earbuds\b', r'\bearbuds\b',
    r'\btv mount\b', r'\bmonitor stand\b', r'\bdesk mount\b', r'\bwall mount\b',
    r'\bmount\b', r'\bbracket\b', r'\bplate\b', r'\bscrew\b',
    r'\bcase\b', r'\bbag\b', r'\bcover\b', r'\bsleeve\b', r'\bstand\b',
    r'\bcart\b', r'\brack\b', r'\bshelf\b', r'\btray\b',
    r'\bsamsung\b.*\btv\b', r'\bsamsung\b.*\btelevision\b',
    r'\blg\b.*\btv\b', r'\bsony\b.*\btv\b', r'\btcl\b', r'\bhisense\b',
]

# Passive consumer accessory patterns to skip
# BUT keep pro AV signal-chain gear
ACCESSORY_SKIP = [
    r'\bhdmi audio stripper\b',
    r'\bhdmi splitter\b',      # passive splitter, not a matrix
    r'\busb hub\b',             # IT usb hub
    r'\busb-c hub\b',
    r'\blightning to\b',       # phone adapter
    r'\busb to\b',             # generic usb adapter
    r'\bdisplayport to\b',     # passive DP adapter
    r'\bhdmi to\b',            # passive HDMI adapter
    r'\bcable\b',              # plain cable
    r'\bcord\b',
]

# KEEP patterns: active signal-chain / infrastructure gear
# These override accessory_skip
INFRASTRUCTURE_KEEP = [
    r'\bextender\b',           # HDBaseT, fiber extenders
    r'\brepeater\b',           # SDI repeaters, signal boosters
    r'\bactive\b.*\bcable\b',  # active optical cable
    r'\bfiber\b',              # fiber transmitter/receiver
    r'\bhdbaset\b',            # HDBaseT
    r'\bsdi\b.*\brepeater\b',
    r'\bdante\b.*\badapter\b', # Dante adapters (e.g. Dante AVIO)
    r'\bdante\b.*\binterface\b',
    r'\bavio\b',               # Audinate AVIO series
    r'\bkvm\b.*\bextender\b',
    r'\bsignal\b.*\bbooster\b',
    r'\bline\b.*\bdriver\b',
    r'\bdi\b.*\bbox\b',       # DI boxes are legit
]

# Generic monitor descriptions to skip
MONITOR_SKIP = [
    r'\bmonitor\s+\d+\b',
    r'\bdisplay\s+\d+\b',
    r'\bscreen\s+\d+\b',
    r'^\d+\"?\s*monitor',
    r'^\d+\"?\s*display',
]

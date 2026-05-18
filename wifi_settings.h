#ifndef WIFI_SETTINGS_H
#define WIFI_SETTINGS_H

/* Debug level */
#define WIFI_IF_DEBUG_LEVEL         E_INFO

/* ========================= */
/* DISABLE PROVISIONING      */
/* ========================= */
#define PROVISIONING_MODE           WifiProvMode_OFF
#define PROVISIONING_CMD            SL_WLAN_PROVISIONING_CMD_START_MODE_APSC

#define PROVISIONING_TIMEOUT        0
#define PROVISIONING_AP_PASSWORD    "1234567890"
#define PROVISIONING_SC_KEY         "1234567890123456"

/* ========================= */
/* FORCE DIRECT CONNECTION   */
/* ========================= */
#define FORCE_PROVISIONING          (0)

/* ========================= */
/* YOUR WIFI SETTINGS        */
/* ========================= */
#define AP_SSID                     "Aravind"
#define AP_PASSWORD                 "Arav02091999"

/* ========================= */
/* CONFIG FILE (NOT USED)    */
/* ========================= */
#define AP_CFG_FILENAME             "network.cfg"
#define AP_CFG_TOKEN                12345678
#define AP_CFG_MAX_SIZE             100

#endif

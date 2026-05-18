/*
 * Copyright (C) 2016-2021, Texas Instruments Incorporated
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * *  Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * *  Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * *  Neither the name of Texas Instruments Incorporated nor the names of
 *    its contributors may be used to endorse or promote products derived
 *    from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
 * EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*****************************************************************************

   Application Name     -   MQTT Client
   Application Overview -   The device is running a MQTT client which is
                           connected to the online broker. Three LEDs on the
                           device can be controlled from a web client by
                           publishing msg on appropriate topics. Similarly,
                           message can be published on pre-configured topics
                           by pressing the switch buttons on the device.

   Application Details  - Refer to 'MQTT Client' README.html

*****************************************************************************/
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <mqueue.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <ti/drivers/SPI.h>
#include <ti/drivers/GPIO.h>
#include <ti/drivers/Timer.h>
#include <ti/drivers/net/wifi/simplelink.h>
#include <ti/drivers/net/wifi/slnetifwifi.h>
#include <ti/drivers/net/wifi/slwificonn.h>
#include <ti/net/slnet.h>
#include <ti/net/slnetif.h>
#include <ti/net/slnetconn.h>
#include <ti/net/mqtt/mqttclient.h>
#include <ti/sysbios/knl/Clock.h>
#include <ti/sysbios/knl/Task.h>
#include <ti/sysbios/knl/Semaphore.h>

#include <ti/sysbios/knl/Queue.h>
#include <ti/sysbios/knl/Semaphore.h>
#include <ti/sysbios/knl/Task.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    Queue_Elem elem;
    uint8_t data[7];
} DataMsg;

Queue_Handle queue1;   // MQTT → Processing
Queue_Handle queue2;   // Processing → Output

Semaphore_Handle sem1;
Semaphore_Handle sem2;

/* ===== MUTEX ===== */
Semaphore_Handle pubMutex;

/* ===== TIMER (timeout detection) ===== */
#define MAX_NODES 4
volatile uint32_t lastSeen[MAX_NODES] = {0};

/* ===== EVENT FLAGS (optional later) ===== */
#define NODE_TIMEOUT_TICKS 5000   // adjust based on your Clock tick rate

#include <ti/sysbios/BIOS.h>
#include "ifmod/uart_if.h"
#include "ifmod/wifi_if.h"
#include "ifmod/mqtt_if.h"
#include "ifmod/debug_if.h"
#include "ifmod/ota_if.h"
#include "ifmod/utils_if.h"
#include "ifmod/ota_vendors.h"

#include "ti_drivers_config.h"

#include <ti/sysbios/BIOS.h>
#include <ti/sysbios/knl/Clock.h>
#include <ti/drivers/GPIO.h>
#include "ti_drivers_config.h"

#include <ti/sysbios/knl/Event.h>

/* void statsTask(UArg arg0, UArg arg1); */
/* Debug controls */
#define DBG_ENABLE   1

#if DBG_ENABLE
#define DBG_LOG(tag, fmt, ...) \
    LOG_INFO("[%s][%lu ms] " fmt, tag, Clock_getTicks(), ##__VA_ARGS__)
#else
#define DBG_LOG(tag, fmt, ...)
#endif

int parsePayload(char *payload, uint8_t *data);
uint16_t crc16(uint8_t *data, int len);

/* ===== EVENT ===== */
Event_Handle eventHandle;

#define HIGH_TEMP_EVENT   Event_Id_00

/* ===== THRESHOLD CONFIG ===== */
#define TEMP_THRESHOLD    32.0f      // adjust as needed
#define TEMP_DURATION_TICKS  3000    // ~3 sec (depends on Clock tick)

/* ===== TRACKING ===== */
uint32_t highStartTime[MAX_NODES] = {0};

typedef enum
{
    ALERT_NORMAL = 0,
    ALERT_WARNING,
    ALERT_CRITICAL,
    ALERT_OFFLINE
} AlertState;

volatile AlertState nodeState[MAX_NODES] = {ALERT_NORMAL};
float latestTemp[MAX_NODES] = {0};
#undef DEBUG_IF_NAME
#define DEBUG_IF_NAME       "MQTT_APP"
#undef DEBUG_IF_SEVERITY
#define DEBUG_IF_SEVERITY   E_INFO

extern int32_t ti_net_SlNet_initConfig();

MQTTClient_Handle gMqttClientHandle = NULL;

static void StartOTA(char* topic, char* payload, uint8_t qos);
static void StartCloudOTA();
static void StartLocalOTA();
static void StartInternalUpdate();

pthread_t gSlNetConnThread;
bool gNewImageLoaded = false;
#define SLNETCONN_TIMEOUT               10 // 10 Second Timeout
#define SLNETCONN_TASK_STACK_SIZE       (1200)


#define APPLICATION_NAME         "MQTT client"
#define APPLICATION_VERSION      "2.0.3"

// un-comment this if you want to connect to an MQTT broker securely
//#define MQTT_SECURE_CLIENT

#define MQTT_MODULE_TASK_PRIORITY   2
#define MQTT_MODULE_TASK_STACK_SIZE 2048

#define MQTT_WILL_TOPIC             "cc32xx_will_topic"
#define MQTT_WILL_MSG               "will_msg_works"
#define MQTT_WILL_QOS               MQTT_QOS_2
#define MQTT_WILL_RETAIN            false

#define MQTT_CLIENT_PASSWORD        NULL
#define MQTT_CLIENT_USERNAME        NULL
#define MQTT_CLIENT_KEEPALIVE       0
#define MQTT_CLIENT_CLEAN_CONNECT   true
#define MQTT_CLIENT_MQTT_V3_1       true
#define MQTT_CLIENT_BLOCKING_SEND   true

#ifndef MQTT_SECURE_CLIENT
#define MQTT_CONNECTION_FLAGS           MQTTCLIENT_NETCONN_URL
#define MQTT_CONNECTION_ADDRESS         "10.18.192.113" //"mqtt.eclipse.org"
#define MQTT_CONNECTION_PORT_NUMBER     1883
#else
#define MQTT_CONNECTION_FLAGS           MQTTCLIENT_NETCONN_IP4 | MQTTCLIENT_NETCONN_SEC
#define MQTT_CONNECTION_ADDRESS         "192.168.178.67"
#define MQTT_CONNECTION_PORT_NUMBER     8883
#endif

//#define OTA_DEFAULT_METHOD              StartCloudOTA
//#define OTA_DEFAULT_METHOD              StartLocalOTA
//#define OTA_DEFAULT_METHOD              StartInternalUpdate

mqd_t appQueue;
volatile int connected;
volatile int deinit;
Timer_Handle timer0;
volatile int longPress = 0;

volatile uint32_t rxCount = 0;
volatile uint32_t procCount = 0;
volatile uint32_t outCount = 0;

/* Optional queue depth (approx) */
volatile int q1Depth = 0;
volatile int q2Depth = 0;
/* Client ID                                                                 */
/* If ClientId isn't set, the MAC address of the device will be copied into  */
/* the ClientID parameter.                                                   */
char ClientId[13] = "clientId123";

enum{
    APP_MQTT_PUBLISH,
    APP_MQTT_CON_TOGGLE,
    APP_MQTT_DEINIT,
    APP_BTN_HANDLER,
    APP_OTA_TRIGGER,
};

struct msgQueue
{
    int   event;
    char* payload;
};

MQTT_IF_InitParams_t mqttInitParams =
{
     MQTT_MODULE_TASK_STACK_SIZE,   // stack size for mqtt module - default is 2048
     MQTT_MODULE_TASK_PRIORITY      // thread priority for MQTT   - default is 2
};

MQTTClient_Will mqttWillParams =
{
     MQTT_WILL_TOPIC,    // will topic
     MQTT_WILL_MSG,      // will message
     MQTT_WILL_QOS,      // will QoS
     MQTT_WILL_RETAIN    // retain flag
};

MQTT_IF_ClientParams_t mqttClientParams =
{
     ClientId,                  // client ID
     MQTT_CLIENT_USERNAME,      // user name
     MQTT_CLIENT_PASSWORD,      // password
     MQTT_CLIENT_KEEPALIVE,     // keep-alive time
     MQTT_CLIENT_CLEAN_CONNECT, // clean connect flag
     MQTT_CLIENT_MQTT_V3_1,     // true = 3.1, false = 3.1.1
     MQTT_CLIENT_BLOCKING_SEND, // blocking send flag
     &mqttWillParams            // will parameters
};

#ifndef MQTT_SECURE_CLIENT
MQTTClient_ConnParams mqttConnParams =
{
     MQTT_CONNECTION_FLAGS,         // connection flags
     MQTT_CONNECTION_ADDRESS,       // server address
     MQTT_CONNECTION_PORT_NUMBER,   // port number of MQTT server
     0,                             // method for secure socket
     0,                             // cipher for secure socket
     0,                             // number of files for secure connection
     NULL                           // secure files
};
#else
/*
 * In order to connect to an MQTT broker securely, the MQTTCLIENT_NETCONN_SEC flag,
 * method for secure socket, cipher, secure files, number of secure files must be set
 * and the certificates must be programmed to the file system.
 *
 * The first parameter is a bit mask which configures the server address type and security mode.
 * Server address type: IPv4, IPv6 and URL must be declared with the corresponding flag.
 * All flags can be found in mqttclient.h.
 *
 * The flag MQTTCLIENT_NETCONN_SEC enables the security (TLS) which includes domain name
 * verification and certificate catalog verification. Those verifications can be skipped by
 * adding to the bit mask: MQTTCLIENT_NETCONN_SKIP_DOMAIN_NAME_VERIFICATION and
 * MQTTCLIENT_NETCONN_SKIP_CERTIFICATE_CATALOG_VERIFICATION.
 *
 * Note: The domain name verification requires URL Server address type otherwise, this
 * verification will be disabled.
 *
 * Secure clients require time configuration in order to verify the server certificate validity (date)
 */

/* Day of month (DD format) range 1-31                                       */
#define DAY                      1
/* Month (MM format) in the range of 1-12                                    */
#define MONTH                    5
/* Year (YYYY format)                                                        */
#define YEAR                     2020
/* Hours in the range of 0-23                                                */
#define HOUR                     4
/* Minutes in the range of 0-59                                              */
#define MINUTES                  00
/* Seconds in the range of 0-59                                              */
#define SEC                      00

char *MQTTClient_secureFiles[1] = {"ca-cert.pem"};

MQTTClient_ConnParams mqttConnParams =
{
    MQTT_CONNECTION_FLAGS,                  // connection flags
    MQTT_CONNECTION_ADDRESS,                // server address
    MQTT_CONNECTION_PORT_NUMBER,            // port number of MQTT server
    SLNETSOCK_SEC_METHOD_SSLv3_TLSV1_2,     // method for secure socket
    SLNETSOCK_SEC_CIPHER_FULL_LIST,         // cipher for secure socket
    1,                                      // number of files for secure connection
    MQTTClient_secureFiles                  // secure files
};

void setTime() {

    SlDateTime_t dateTime = {0};
    dateTime.tm_day = (uint32_t)DAY;
    dateTime.tm_mon = (uint32_t)MONTH;
    dateTime.tm_year = (uint32_t)YEAR;
    dateTime.tm_hour = (uint32_t)HOUR;
    dateTime.tm_min = (uint32_t)MINUTES;
    dateTime.tm_sec = (uint32_t)SEC;
    sl_DeviceSet(SL_DEVICE_GENERAL, SL_DEVICE_GENERAL_DATE_TIME,
                 sizeof(SlDateTime_t), (uint8_t *)(&dateTime));
}
#endif

//*****************************************************************************
//
//! \brief The Function Handles the Fatal errors
//!
//! \param[in]  slFatalErrorEvent - Pointer to Fatal Error Event info
//!
//! \return None
//!
//*****************************************************************************

void SimpleLinkSockEventHandler(SlSockEvent_t *pSock)
{
    if ( pSock->Event == SL_SOCKET_ASYNC_EVENT)
    {
        switch (pSock->SocketAsyncEvent.SockAsyncData.Type)
        {
        case SL_SSL_NOTIFICATION_WRONG_ROOT_CA:
            /* on socket error Restart OTA */
            LOG_INFO("SL_SOCKET_ASYNC_EVENT: ERROR - WRONG ROOT CA");
            LOG_INFO("Please install the following Root Certificate:");
            LOG_INFO(" %s\n\r", pSock->SocketAsyncEvent.SockAsyncData.pExtraInfo);
            break;
        default:
            /* on socket error Restart OTA */
            LOG_INFO("SL_SOCKET_ASYNC_EVENT socket event %d", pSock->Event);
        }
    }
}

void SimpleLinkHttpServerEventHandler(
        SlNetAppHttpServerEvent_t *pHttpEvent,
        SlNetAppHttpServerResponse_t *
        pHttpResponse)
{
    /* Unused in this application */
}

void SimpleLinkNetAppRequestMemFreeEventHandler(_u8 *buffer)
{
    /* Unused in this application */
}

//*****************************************************************************
//!
//! Set the ClientId with its own mac address
//! This routine converts the mac address which is given
//! by an integer type variable in hexadecimal base to ASCII
//! representation, and copies it into the ClientId parameter.
//!
//! \param  macAddress  -   Points to string Hex.
//!
//! \return void.
//!
//*****************************************************************************
int32_t SetClientIdNamefromMacAddress()
{
    int32_t ret = 0;
    uint8_t Client_Mac_Name[2];
    uint8_t Index;
    uint16_t macAddressLen = SL_MAC_ADDR_LEN;
    uint8_t macAddress[SL_MAC_ADDR_LEN];

    /*Get the device Mac address */
    ret = sl_NetCfgGet(SL_NETCFG_MAC_ADDRESS_GET, 0, &macAddressLen,
                       &macAddress[0]);

    /*When ClientID isn't set, use the mac address as ClientID               */
    if(ClientId[0] == '\0')
    {
        /*6 bytes is the length of the mac address                           */
        for(Index = 0; Index < SL_MAC_ADDR_LEN; Index++)
        {
            /*Each mac address byte contains two hexadecimal characters      */
            /*Copy the 4 MSB - the most significant character                */
            Client_Mac_Name[0] = (macAddress[Index] >> 4) & 0xf;
            /*Copy the 4 LSB - the least significant character               */
            Client_Mac_Name[1] = macAddress[Index] & 0xf;

            if(Client_Mac_Name[0] > 9)
            {
                /*Converts and copies from number that is greater than 9 in  */
                /*hexadecimal representation (a to f) into ascii character   */
                ClientId[2 * Index] = Client_Mac_Name[0] + 'a' - 10;
            }
            else
            {
                /*Converts and copies from number 0 - 9 in hexadecimal       */
                /*representation into ascii character                        */
                ClientId[2 * Index] = Client_Mac_Name[0] + '0';
            }
            if(Client_Mac_Name[1] > 9)
            {
                /*Converts and copies from number that is greater than 9 in  */
                /*hexadecimal representation (a to f) into ascii character   */
                ClientId[2 * Index + 1] = Client_Mac_Name[1] + 'a' - 10;
            }
            else
            {
                /*Converts and copies from number 0 - 9 in hexadecimal       */
                /*representation into ascii character                        */
                ClientId[2 * Index + 1] = Client_Mac_Name[1] + '0';
            }
        }
    }
    return(ret);
}

void timerCallback(Timer_Handle myHandle)
{
    longPress = 1;
}

// this timer callback toggles the LED once per second until the device connects to an AP
void timerLEDCallback(Timer_Handle myHandle)
{
    GPIO_toggle(CONFIG_GPIO_LED_0);
}

void pushButtonPublishHandler(uint_least8_t index)
{
    int ret;
    struct msgQueue queueElement;

    GPIO_disableInt(CONFIG_GPIO_BUTTON_0);

    queueElement.event = APP_MQTT_PUBLISH;
    ret = mq_send(appQueue, (const char*)&queueElement, sizeof(struct msgQueue), 0);
    if(ret < 0){
        LOG_ERROR("msg queue send error %d", ret);
    }
    queueElement.event = APP_OTA_TRIGGER;
    ret = mq_send(appQueue, (const char*)&queueElement, sizeof(struct msgQueue), 0);
    if(ret < 0){
        LOG_ERROR("msg queue send error %d", ret);
    }
}

void pushButtonConnectionHandler(uint_least8_t index)
{
    int ret;
    struct msgQueue queueElement;

    GPIO_disableInt(CONFIG_GPIO_BUTTON_1);

    ret = Timer_start(timer0);
    if(ret < 0){
        LOG_ERROR("failed to start the timer\r\n");
    }

    queueElement.event = APP_BTN_HANDLER;

    ret = mq_send(appQueue, (const char*)&queueElement, sizeof(struct msgQueue), 0);
    if(ret < 0){
        LOG_ERROR("msg queue send error %d", ret);
    }
}

int detectLongPress(){

    int buttonPressed;

    do{
        buttonPressed = GPIO_read(CONFIG_GPIO_BUTTON_1);
    }while(buttonPressed && !longPress);

    // disabling the timer in case the callback has not yet triggered to avoid updating longPress
    Timer_stop(timer0);

    if(longPress == 1){
        longPress = 0;
        return 1;
    }
    else{
        return 0;
    }
}


void MQTT_EventCallback(int32_t event){

    struct msgQueue queueElement;

    switch(event){

        case MQTT_EVENT_CONNACK:
        {
            deinit = 0;
            connected = 1;
            LOG_INFO("MQTT_EVENT_CONNACK\r\n");
            GPIO_clearInt(CONFIG_GPIO_BUTTON_1);
            GPIO_enableInt(CONFIG_GPIO_BUTTON_1);
            break;
        }

        case MQTT_EVENT_SUBACK:
        {
            LOG_INFO("MQTT_EVENT_SUBACK\r\n");
            break;
        }

        case MQTT_EVENT_PUBACK:
        {
            LOG_INFO("MQTT_EVENT_PUBACK\r\n");
            break;
        }

        case MQTT_EVENT_UNSUBACK:
        {
            LOG_INFO("MQTT_EVENT_UNSUBACK\r\n");
            break;
        }

        case MQTT_EVENT_CLIENT_DISCONNECT:
        {
            connected = 0;
            LOG_INFO("MQTT_EVENT_CLIENT_DISCONNECT\r\n");
            if(deinit == 0){
                GPIO_clearInt(CONFIG_GPIO_BUTTON_1);
                GPIO_enableInt(CONFIG_GPIO_BUTTON_1);
            }
            break;
        }

        case MQTT_EVENT_SERVER_DISCONNECT:
        {
            connected = 0;

            LOG_INFO("MQTT_EVENT_SERVER_DISCONNECT\r\n");

            queueElement.event = APP_MQTT_CON_TOGGLE;
            int res = mq_send(appQueue, (const char*)&queueElement, sizeof(struct msgQueue), 0);
            if(res < 0){
                LOG_ERROR("msg queue send error %d", res);
            }
            break;
        }

        case MQTT_EVENT_DESTROY:
        {
            LOG_INFO("MQTT_EVENT_DESTROY\r\n");
            break;
        }
    }
}

/*
 * Subscribe topic callbacks
 * Topic and payload data is deleted after topic callbacks return.
 * User must copy the topic or payload data if it needs to be saved.
 */

void BrokerCB(char* topic, char* payload, uint8_t qos)
{
    uint8_t data[7];

    if(payload == NULL)
        return;

    char localPayload[50];

    memset(localPayload, 0, sizeof(localPayload));
    strncpy(localPayload, payload, sizeof(localPayload)-1);

    if(parsePayload(localPayload, data) < 0)
        return;

    if(data[0] != 0x7E || data[6] != 0xE7)
        return;

    uint8_t nodeID = data[1];

    if(nodeID < 1 || nodeID > 3)
        return;

    int tempRaw = (data[2] << 8) | data[3];

    float temp = tempRaw / 100.0;

    latestTemp[nodeID] = temp;

    lastSeen[nodeID] = Clock_getTicks();

    rxCount++;

    /* ALERT STATE */

    if(temp < 32.0)
    {
        nodeState[nodeID] = ALERT_NORMAL;
    }
    else if(temp >= 32.0 && temp <= 38.0)
    {
        nodeState[nodeID] = ALERT_WARNING;
    }
    else
    {
        nodeState[nodeID] = ALERT_CRITICAL;
    }

    LOG_INFO("Node %d | Temp %.2f C\r\n", nodeID, temp);
}

void ToggleLED1CB(char* topic, char* payload, uint8_t qos){
    GPIO_toggle(CONFIG_GPIO_LED_0);
    LOG_INFO("TOPIC: %s PAYLOAD: %s QOS: %d\r\n", topic, payload, qos);
}

void ToggleLED2CB(char* topic, char* payload, uint8_t qos){
    GPIO_toggle(CONFIG_GPIO_LED_1);
    LOG_INFO("TOPIC: %s PAYLOAD: %s QOS: %d\r\n", topic, payload, qos);
}

void ToggleLED3CB(char* topic, char* payload, uint8_t qos){
    GPIO_toggle(CONFIG_GPIO_LED_2);
    LOG_INFO("TOPIC: %s PAYLOAD: %s QOS: %d\r\n", topic, payload, qos);
}

int32_t DisplayAppBanner(char* appName, char* appVersion){

    int32_t ret = 0;
    uint8_t macAddress[SL_MAC_ADDR_LEN];
    uint16_t macAddressLen = SL_MAC_ADDR_LEN;
    uint16_t ConfigSize = 0;
    uint8_t ConfigOpt = SL_DEVICE_GENERAL_VERSION;
    SlDeviceVersion_t ver = {0};

    ConfigSize = sizeof(SlDeviceVersion_t);

    // get the device version info and MAC address
    ret = sl_DeviceGet(SL_DEVICE_GENERAL, &ConfigOpt, &ConfigSize, (uint8_t*)(&ver));
    ret |= (int32_t)sl_NetCfgGet(SL_NETCFG_MAC_ADDRESS_GET, 0, &macAddressLen, &macAddress[0]);

    UART_PRINT("\n\r\t============================================\n\r");
    UART_PRINT("\t   %s Example Ver: %s",appName, appVersion);
    UART_PRINT("\n\r\t============================================\n\r\n\r");

    UART_PRINT("\t CHIP: 0x%x\n\r",ver.ChipId);
    UART_PRINT("\t MAC:  %d.%d.%d.%d\n\r",ver.FwVersion[0],ver.FwVersion[1],
               ver.FwVersion[2],
               ver.FwVersion[3]);
    UART_PRINT("\t PHY:  %d.%d.%d.%d\n\r",ver.PhyVersion[0],ver.PhyVersion[1],
               ver.PhyVersion[2],
               ver.PhyVersion[3]);
    UART_PRINT("\t NWP:  %d.%d.%d.%d\n\r",ver.NwpVersion[0],ver.NwpVersion[1],
               ver.NwpVersion[2],
               ver.NwpVersion[3]);
    UART_PRINT("\t ROM:  %d\n\r",ver.RomVersion);
    UART_PRINT("\t HOST: %s\n\r", SL_DRIVER_VERSION);
    UART_PRINT("\t MAC address: %02x:%02x:%02x:%02x:%02x:%02x\r\n", macAddress[0],
               macAddress[1], macAddress[2], macAddress[3], macAddress[4],
               macAddress[5]);
    UART_PRINT("\n\r\t============================================\n\r");

    return(ret);
}

//*****************************************************************************
//
//! \brief  SlWifiConn Event Handler
//!
//*****************************************************************************
static void SlNetConnEventHandler(uint32_t ifID, SlNetConnStatus_e netStatus, void* data)
{
    switch(netStatus)
    {
    case SLNETCONN_STATUS_CONNECTED_MAC:
        UART_PRINT("[SlNetConnEventHandler] I/F %d - CONNECTED (MAC LEVEL)!\n\r", ifID);
    break;
    case SLNETCONN_STATUS_CONNECTED_IP:
        UART_PRINT("[SlNetConnEventHandler] I/F %d - CONNECTED (IP LEVEL)!\n\r", ifID);
    break;
    case SLNETCONN_STATUS_CONNECTED_INTERNET:
        UART_PRINT("[SlNetConnEventHandler] I/F %d - CONNECTED (INTERNET LEVEL)!\n\r", ifID);
    break;
    case SLNETCONN_STATUS_WAITING_FOR_CONNECTION:
    case SLNETCONN_STATUS_DISCONNECTED:
        UART_PRINT("[SlNetConnEventHandler] I/F %d - DISCONNECTED!\n\r", ifID);
    break;
    default:
        UART_PRINT("[SlNetConnEventHandler] I/F %d - UNKNOWN STATUS\n\r", ifID);
    break;
    }
}

#if OTA_SUPPORT

void OtaCallback(otaNotif_e notification, OtaEventParam_u *pParams)
{

    SlNetConnStatus_e status;
    int retVal;

    switch(notification)
    {
    case OTA_NOTIF_IMAGE_PENDING_COMMIT:
         LOG_INFO("OTA_NOTIF_IMAGE_PENDING_COMMIT");
         retVal = SlNetConn_getStatus(true, &status);
         if (retVal == 0 && status == SLNETCONN_STATUS_CONNECTED_INTERNET)
         {
             OTA_IF_commit();
         }
         else
         {
             OTA_IF_rollback();
             LOG_ERROR("Error Testing the new version - reverting to old version (%d)", retVal);
         }
         break;
    case OTA_NOTIF_IMAGE_DOWNLOADED:
    {
        struct msgQueue queueElement;
        gNewImageLoaded = true;
        LOG_INFO("OTA_NOTIF_IMAGE_DOWNLOADED");
        /* Closing the MQTT - will take the main thread out of its execution loop, then
         * the gNewImageLoaded will mark that new Update is ready for installation
         * (which involves MCU Reset)
         */
        queueElement.event = APP_MQTT_DEINIT;
        retVal = mq_send(appQueue, (const char*)&queueElement, sizeof(struct msgQueue), 0);
        assert (retVal == 0);
        break;
    }
    case OTA_NOTIF_GETLINK_ERROR:
        LOG_ERROR("OTA_NOTIF_GETLINK_ERROR (%d)", pParams->err.errorCode);
        break;
    case OTA_NOTIF_DOWNLOAD_ERROR:
        LOG_ERROR("OTA_NOTIF_DOWNLOAD_ERROR (%d)", pParams->err.errorCode);
        break;
    case OTA_NOTIF_INSTALL_ERROR:
        LOG_ERROR("OTA_NOTIF_INSTALL_ERROR (%d)", pParams->err.errorCode);
        break;
   case OTA_NOTIF_COMMIT_ERROR:
         LOG_ERROR("OTA_NOTIF_COMMIT_ERROR {%d)", pParams->err.errorCode);
         break;
    default:
        LOG_INFO("OTA_NOTIF not handled Id %d", notification);
        break;
    }
}
#endif

static void StartCloudOTA()
{
#if CLOUD_OTA_SUPPORT
    LOG_INFO("Starting Cloud OTA");
    OTA_IF_downloadImageByCloudVendor(OTA_GITHUB_getDownloadLink, OTA_DROPBOX_getDownloadLink, 0);
#else
    LOG_WARNING("Cloud OTA is not enabled. Please enable CLOUD_OTA_SUPPORT macro in ota_settings.h");
#endif
}

static void StartLocalOTA()
{
#if LOCAL_OTA_SUPPORT
    LOG_INFO("Starting Local OTA");
    OTA_IF_uploadImage(0); // use default HTTP port and no security
#else
    LOG_WARNING("Local OTA is not enabled. Enable LOCAL_OTA_SUPPORT macro. Please enable CLOUD_OTA_SUPPORT macro in ota_settings.h");
#endif
}

static void StartInternalUpdate()
{
#if INTERNAL_UPDATE_SUPPORT
    TarFileParams_t tarFileParams;

    tarFileParams.pPath =  "/otaImages/cc3235sf.tar";
    tarFileParams.token = 0;
    tarFileParams.pVersion = NULL;
    LOG_INFO("Starting Internal Update");
    OTA_IF_readImage(&tarFileParams, 0); // use default HTTP port and no security
#else
    LOG_WARNING("Internal Update is not enabled. Please enable INTERNAL_UPDATE_SUPPORT macro in ota_settings.h");
#endif
}

static void StartOTA(char* topic, char* payload, uint8_t qos)
{
    if(topic != NULL)
    {
        LOG_INFO("Trigger OTA...(MQTT:: %s)", payload);
        if(strcmp(payload, "cloud") == 0)
        {
            StartCloudOTA();
        }
        else if(strcmp(payload, "local") == 0)
        {
            StartLocalOTA();
        }
        else if(strcmp(payload, "internal") == 0)
        {
            StartInternalUpdate();
        }
        else
        {
            /* Unknown payload - mark as NULL to use the default method */
            LOG_WARNING("Unknown payload - using the default OTA method");
            topic = NULL;
        }
    }
    else
    {
        LOG_INFO("Trigger OTA...(button press)");
    }
    /* The following is the default for button press or unknown MQTT payload */
    if(topic == NULL)
    {
#ifdef OTA_DEFAULT_METHOD
        OTA_DEFAULT_METHOD();
#else
        LOG_WARNING("No default OTA method is defined.");
        LOG_WARNING("Set OTA_DEFAULT_METHOD to one of: StartCloudOTA / StartLocalOTA / StartInternalUpdate");

#endif
    }
}



/************ HEX STRING → BYTE ARRAY ************/
int parsePayload(char *payload, uint8_t *data)
{
    int count = 0;

    char *token = strtok(payload, " ");

    while(token != NULL && count < 7)
    {
        data[count++] = (uint8_t)strtol(token, NULL, 16);
        token = strtok(NULL, " ");
    }

    if(count != 7)
    {
        LOG_ERROR("Payload parsing failed! Count=%d\r\n", count);
        return -1;
    }

    return 0;
}

/************ CRC16 (SAME AS ESP32) ************/
uint16_t crc16(uint8_t *data, int len)
{
    uint16_t crc = 0xFFFF;
    for(int i = 0; i < len; i++)
    {
        crc ^= data[i];
        for(int j = 0; j < 8; j++)
        {
            if(crc & 1)
                crc = (crc >> 1) ^ 0xA001;
            else
                crc >>= 1;
        }
    }
    return crc;
}

void processingTask(UArg arg0, UArg arg1)
{
    while(1)
    {
        Semaphore_pend(sem1, BIOS_WAIT_FOREVER);

        /* FIRST get data */
        Queue_Elem *elem = Queue_get(queue1);
        DataMsg *msg = (DataMsg *)elem;

        /* THEN update debug */
        procCount++;
        q1Depth--;

        /* DBG_LOG("PROC", "Dequeue Q1 → depth=%d, PROC #%lu\r\n", q1Depth, procCount); */


        uint8_t *data = msg->data;

   /*     if(data[0] != 0x7E || data[6] != 0xE7)
        {
            LOG_ERROR("Invalid frame\r\n");
            free(msg);
            continue;
        }
*/
        uint8_t nodeID = data[1];
        lastSeen[nodeID] = Clock_getTicks();
        int tempRaw = (data[2] << 8) | data[3];
        float temp = tempRaw / 100.0;
        latestTemp[nodeID] = temp;


        uint16_t recv_crc = (data[4] << 8) | data[5];
        uint16_t calc_crc = crc16(&data[1], 3);

       /* DBG_LOG("PROC", "node=%d temp=%.2f CRC=%s\r\n",
                nodeID, temp, (recv_crc == calc_crc) ? "OK" : "BAD"); */

        msg->data[0] = nodeID;

        /* store temp separately */
        float *tempPtr = (float *)&msg->data[1];
        *tempPtr = temp;

        /* store CRC status */
        msg->data[5] = (recv_crc == calc_crc) ? 1 : 0;

        q2Depth++;
        /* DBG_LOG("PROC", "Enqueued → Q2 depth=%d\r\n", q2Depth); */

        Queue_put(queue2, &msg->elem);
        Semaphore_post(sem2);

        uint32_t now = Clock_getTicks();

        /* Node recovered from offline */
        if(nodeState[nodeID] == ALERT_OFFLINE)
        {
            LOG_INFO("[RECOVERY] Node %d ONLINE\r\n", nodeID);
        }

        /* Threshold logic */

        /* ===== ALERT STATE MACHINE ===== */

        if(temp < 32.0)
        {
            nodeState[nodeID] = ALERT_NORMAL;
        }
        else if(temp >= 32.0 && temp <= 38.0)
        {
            nodeState[nodeID] = ALERT_WARNING;
        }
        else
        {
            nodeState[nodeID] = ALERT_CRITICAL;
        }
    }
}


void statsTask(UArg arg0, UArg arg1)
{
    while(1)
    {
        LOG_INFO("\n===== GATEWAY STATUS =====\r\n");

        for(int i=1; i<=3; i++)
        {
            char *state;

            switch(nodeState[i])
            {
                case ALERT_NORMAL:
                    state = "NORMAL";
                    break;

                case ALERT_WARNING:
                    state = "WARNING";
                    break;

                case ALERT_CRITICAL:
                    state = "CRITICAL";
                    break;

                case ALERT_OFFLINE:
                    state = "OFFLINE";
                    break;

                default:
                    state = "UNKNOWN";
                    break;
            }

            LOG_INFO("Node %d | %.2f C | %s\r\n",
                     i,
                     latestTemp[i],
                     state);
        }

        LOG_INFO("==========================\r\n");

        Task_sleep(5000);
    }
}


void actuatorTask(UArg arg0, UArg arg1)
{
    static uint32_t lastToggle[4] = {0};

    while(1)
    {
        uint32_t now = Clock_getTicks();

        for(int i = 1; i <= 3; i++)
        {
            uint_least8_t led;

            /* Node → LED mapping */
            if(i == 1)
                led = CONFIG_GPIO_LED_0;

            else if(i == 2)
                led = CONFIG_GPIO_LED_1;

            else
                led = CONFIG_GPIO_LED_2;

            switch(nodeState[i])
            {
                case ALERT_NORMAL:
                {
                    GPIO_write(led, 0);
                    break;
                }

                case ALERT_WARNING:
                {
                    /* Slow blink */
                    if((now - lastToggle[i]) > 500)
                    {
                        GPIO_toggle(led);
                        lastToggle[i] = now;
                    }
                    break;
                }

                case ALERT_CRITICAL:
                {
                    /* Fast blink */
                    if((now - lastToggle[i]) > 100)
                    {
                        GPIO_toggle(led);
                        lastToggle[i] = now;
                    }
                    break;
                }

                case ALERT_OFFLINE:
                {
                    /* Solid ON */
                    GPIO_write(led, 1);
                    break;
                }

                default:
                    break;
            }
        }

        Task_sleep(20);
    }
}

void outputTask(UArg arg0, UArg arg1)
{
    while(1)
    {
        Semaphore_pend(sem2, BIOS_WAIT_FOREVER);

        /* FIRST get data */
        Queue_Elem *elem = Queue_get(queue2);
        DataMsg *msg = (DataMsg *)elem;

        /* THEN debug */
        outCount++;
        q2Depth--;

       /* DBG_LOG("OUT", "Dequeue Q2 → depth=%d, OUT #%lu\r\n", q2Depth, outCount); */

        uint8_t nodeID = msg->data[0];

        float temp = 0;
        memcpy(&temp, &msg->data[1], sizeof(float));

        uint8_t status = msg->data[5];

      /*  LOG_INFO("\n===== OUTPUT TASK =====\r\n");
        LOG_INFO("Node ID     : %d\r\n", nodeID);
        LOG_INFO("Temperature : %.2f C\r\n", temp);

        if(status)
        {
            LOG_INFO("CRC Status  : VALID\r\n");
        }
        else
        {
            LOG_ERROR("CRC Status  : INVALID\r\n");
        }

        LOG_INFO("=======================\r\n"); */

        free(msg);

        char payload[100];

           snprintf(payload, sizeof(payload),
                    "node=%d,temp=%.2f,status=%s",
                    nodeID,
                    temp,
                    status ? "valid" : "invalid");

           Semaphore_pend(pubMutex, BIOS_WAIT_FOREVER);

           MQTT_IF_Publish(gMqttClientHandle,
                           "gateway/processed/data",
                           payload,
                           strlen(payload),
                           MQTT_QOS_0);

           Semaphore_post(pubMutex);

          /* DBG_LOG("OUT", "Published → %s\r\n", payload); */
    }
}



Void timeoutCb(UArg arg)
{
    uint32_t now = Clock_getTicks();

    for(int i = 1; i <= 3; i++)
    {
        /* Ignore until node sends first packet */
        if(lastSeen[i] == 0)
        {
            continue;
        }

        if((now - lastSeen[i]) > NODE_TIMEOUT_TICKS)
        {
            nodeState[i] = ALERT_OFFLINE;
        }
    }
}

void mainThread(void * args)
{
    int32_t ret;
    int32_t conn_Failure = 0;
    mq_attr attr;
    Timer_Params params;
    struct msgQueue queueElement;
    MQTTClient_Handle mqttClientHandle = NULL;

    InitTerm();

    GPIO_init();
    SPI_init();
    Timer_init();

    Queue_Params qParams;

    Queue_Params_init(&qParams);
    queue1 = Queue_create(&qParams, NULL);
    queue2 = Queue_create(&qParams, NULL);

    Semaphore_Params sParams;
    Semaphore_Params_init(&sParams);

    sem1 = Semaphore_create(0, &sParams, NULL);
    sem2 = Semaphore_create(0, &sParams, NULL);

    /* Mutex */
    pubMutex = Semaphore_create(1, &sParams, NULL);

    Task_Params taskParams;


    Event_Params eParams;
    Event_Params_init(&eParams);

    eventHandle = Event_create(&eParams, NULL);

    Clock_Params cParams;
    Clock_Params_init(&cParams);

    cParams.period = 1000;
    cParams.startFlag = TRUE;

    Clock_create(timeoutCb, 1000, &cParams, NULL);

    /* Stats Task */
    Task_Params_init(&taskParams);
    taskParams.stackSize = 1024;
    taskParams.priority = 1;
    Task_create(statsTask, &taskParams, NULL);

    /* Processing Task */
    Task_Params_init(&taskParams);
    taskParams.stackSize = 2048;
    taskParams.priority = 3;
   /* Task_create(processingTask, &taskParams, NULL); */

    /* Output Task */
    Task_Params_init(&taskParams);
    taskParams.stackSize = 2048;
    taskParams.priority = 2;
   /* Task_create(outputTask, &taskParams, NULL); */

    /* Actuator Task */
    Task_Params_init(&taskParams);
    taskParams.stackSize = 1024;
    taskParams.priority = 4;   // HIGHEST priority
    Task_create(actuatorTask, &taskParams, NULL);

    GPIO_setCallback(CONFIG_GPIO_BUTTON_0, pushButtonPublishHandler);
    GPIO_setCallback(CONFIG_GPIO_BUTTON_1, pushButtonConnectionHandler);

    GPIO_write(CONFIG_GPIO_LED_0, CONFIG_GPIO_LED_OFF);
    GPIO_write(CONFIG_GPIO_LED_1, CONFIG_GPIO_LED_OFF);
    GPIO_write(CONFIG_GPIO_LED_2, CONFIG_GPIO_LED_OFF);

    // configuring the timer to toggle an LED until the AP is connected
    Timer_Params_init(&params);
    params.period = 1500000;
    params.periodUnits = Timer_PERIOD_US;
    params.timerMode = Timer_ONESHOT_CALLBACK;
    params.timerCallback = (Timer_CallBackFxn)timerCallback;

    timer0 = Timer_open(CONFIG_TIMER_0, &params);
    if (timer0 == NULL) {
        LOG_ERROR("failed to initialize timer\r\n");
        while(1);
    }

    attr.mq_maxmsg = 10;
    attr.mq_msgsize = sizeof(struct msgQueue);
    appQueue = mq_open("appQueue", O_CREAT, 0, &attr);
    if(((int)appQueue) <= 0){
        while(1);
    }

    /* Enable SlNet framework */
    ret = ti_net_SlNet_initConfig();
    if(0 != ret)
    {
        LOG_ERROR("Failed to initialize SlNetSock\n\r");
    }

    /* Enable SlWifiConn */
    ret = WIFI_IF_init();
    assert (ret == 0);

    /* To get the NWP content (version + MAC),
     * The DisplayAppBanner should be called after WIFI_IF_init() after sl_Start() */
    DisplayAppBanner(APPLICATION_NAME, APPLICATION_VERSION);


    /* Enable SlNetConn */
    ret = SlNetConn_init(0);
    assert (ret == 0);

    gSlNetConnThread = OS_createTask(1, SLNETCONN_TASK_STACK_SIZE, SlNetConn_process, NULL, OS_TASK_FLAG_DETACHED);
    assert(gSlNetConnThread);


    ret = SlNetConn_start(SLNETCONN_SERVICE_LVL_INTERNET, SlNetConnEventHandler, SLNETCONN_TIMEOUT, 0);

    /* If Failure to acquire AP Connection, verify no pending OTA commit by initializing OTA library then return to attempting to connect to the AP */
    if(ret != 0)
    {
        LOG_INFO("failed to Connect to AP: Error Code: %d. Verifying no pending OTA commits then re-attempting", ret);
        conn_Failure = 1;

    }

#if OTA_SUPPORT
    HTTPSRV_IF_params_t *pHttpSrvParams = NULL;
#if LOCAL_OTA_SUPPORT
    HTTPSRV_IF_params_t httpsSrvParams;
    httpsSrvParams.pClientRootCa = NULL;
    httpsSrvParams.pServerCert = "dummy-root-ca-cert";
    httpsSrvParams.pServerKey = "dummy-root-ca-cert-key";
    httpsSrvParams.primaryPort = 443;
    httpsSrvParams.secondaryPort = 80;
    pHttpSrvParams = &httpsSrvParams;
#endif
    ret = OTA_IF_init(pHttpSrvParams, OtaCallback, 0, NULL);
    if(ret < 0){
        LOG_INFO("failed to init OTA_IF");
        while(1);
    }
#endif

    /* Loop attempt to establish AP Connection */
    while(conn_Failure != 0)
    {
        conn_Failure = SlNetConn_start(SLNETCONN_SERVICE_LVL_INTERNET, SlNetConnEventHandler, SLNETCONN_TIMEOUT, 0);
        LOG_INFO("failed to Connect to AP: Error Code: %d. Retrying...",conn_Failure);
    }

/* AP Connection Success, continue MQTT Application */
MQTT_DEMO:

    ret = MQTT_IF_Init(mqttInitParams);
    if(ret < 0){
        while(1);
    }

#ifdef MQTT_SECURE_CLIENT
    setTime();
#endif

    /*
     * In case a persistent session is being used, subscribe is called before connect so that the module
     * is aware of the topic callbacks the user is using. This is important because if the broker is holding
     * messages for the client, after CONNACK the client may receive the messages before the module is aware
     * of the topic callbacks. The user may still call subscribe after connect but have to be aware of this.
     */


    do {
        ret = SlNetConn_waitForConnection(SLNETCONN_SERVICE_LVL_INTERNET, SLNETCONN_TIMEOUT);
    } while(ret != 0);
    LOG_INFO("Wi-Fi connection is UP");

    gMqttClientHandle = MQTT_IF_Connect(mqttClientParams, mqttConnParams, MQTT_EventCallback);

    if((int)gMqttClientHandle < 0){
        LOG_ERROR("MQTT_IF_Connect Error (%d)\r\n", mqttClientHandle);
    }
    else
    {

        // wait for CONNACK
        while(connected == 0);
        LOG_INFO("MQTT connection is UP");

        ret = MQTT_IF_Subscribe(gMqttClientHandle,
                                "sensor/+/data",
                                MQTT_QOS_1,
                                BrokerCB);

        ret |= MQTT_IF_Subscribe(gMqttClientHandle,
                                 "cc32xx/ToggleLED2",
                                 MQTT_QOS_2,
                                 ToggleLED2CB);

        ret |= MQTT_IF_Subscribe(gMqttClientHandle,
                                 "cc32xx/ToggleLED3",
                                 MQTT_QOS_2,
                                 ToggleLED3CB);

        ret |= MQTT_IF_Subscribe(gMqttClientHandle,
                                 "cc32xx/OTA",
                                 MQTT_QOS_2,
                                 StartOTA);

        if(ret < 0)
        {
            LOG_ERROR("MQTT subscribe failed\r\n");
        }
        else
        {
            LOG_INFO("All MQTT subscriptions successful\r\n");
        }
        GPIO_enableInt(CONFIG_GPIO_BUTTON_0);

        while(1){

            mq_receive(appQueue, (char*)&queueElement, sizeof(struct msgQueue), NULL);

            if(queueElement.event == APP_MQTT_PUBLISH){

                LOG_INFO("APP_MQTT_PUBLISH\r\n");

                MQTT_IF_Publish(mqttClientHandle,
                                "cc32xx/ToggleLED1",
                                "LED 1 toggle\r\n",
                                strlen("LED 1 toggle\r\n"),
                                MQTT_QOS_2);

                GPIO_clearInt(CONFIG_GPIO_BUTTON_0);
                GPIO_enableInt(CONFIG_GPIO_BUTTON_0);

            }
            else if(queueElement.event == APP_MQTT_CON_TOGGLE){

                LOG_TRACE("APP_MQTT_CON_TOGGLE %d\r\n", connected);


                if(connected){
                    ret = MQTT_IF_Disconnect(mqttClientHandle);
                    if(ret >= 0){
                        connected = 0;
                    }
                }
                else{
                    mqttClientHandle = MQTT_IF_Connect(mqttClientParams, mqttConnParams, MQTT_EventCallback);
                    if((int)mqttClientHandle >= 0)
                    {
                        connected = 1;
                    }
                    /* If failed to re-connect to mqtt start over (this will also include waiting for
                     * the wi-fi connection (in case failure of AP connection caused the disconnection )
                     *  */
                    if(connected == 0)
                        break;
                }
            }
            else if(queueElement.event == APP_MQTT_DEINIT){
                break;
            }
            else if(queueElement.event == APP_OTA_TRIGGER){
                StartOTA(NULL, NULL, 0);
            }
            else if(queueElement.event == APP_BTN_HANDLER){

                struct msgQueue queueElement;

                ret = detectLongPress();
                if(ret == 0){

                    LOG_TRACE("APP_BTN_HANDLER SHORT PRESS\r\n");
                    queueElement.event = APP_MQTT_CON_TOGGLE;
                }
                else{

                    LOG_TRACE("APP_BTN_HANDLER LONG PRESS\r\n");
                    queueElement.event = APP_MQTT_DEINIT;
                }

                ret = mq_send(appQueue, (const char*)&queueElement, sizeof(struct msgQueue), 0);
                if(ret < 0){
                    LOG_ERROR("msg queue send error %d", ret);
                }
            }
        }
    }
    deinit = 1;
    if(connected){
        MQTT_IF_Disconnect(mqttClientHandle);
    }
    MQTT_IF_Deinit();

#if OTA_SUPPORT
    if(gNewImageLoaded)
    {
        SlNetConn_stop(SlNetConnEventHandler);
        OTA_IF_install();
    }
#endif

    LOG_INFO("looping the MQTT functionality of the example for demonstration purposes only\r\n");
    sleep(1);
    goto MQTT_DEMO;
}

//*****************************************************************************
//
// Close the Doxygen group.
//! @}
//
//*****************************************************************************

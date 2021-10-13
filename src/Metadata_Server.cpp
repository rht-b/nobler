/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   Metadata_Server.cpp
 * Author: shahrooz
 *
 * Created on August 26, 2020, 11:17 PM
 */

#include <cstdlib>
#include "Util.h"
#include <map>
#include <string>
#include <sys/socket.h>
//#include <stdlib.h>
#include <netinet/in.h>
#include <vector>
#include <thread>
#include <mutex>
#include "../inc/Data_Transfer.h"
#include <cstdlib>
#include <utility>
#include <sstream>
#include <iostream>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

// Todo: add the timestamp that the reconfiguration happened on to the confid part of the pair. confid!timestamp
//       the server may or may not need it. We will see.

using namespace std;

/*
 * KEY = key
 * VALUE = pair of {Ready-Configuration, ToRetire-Configuration}
 * if no reconfiguration has happened on key!confid, newconfid will be confid and timestamp will be ""
 */

struct Configuration {
    string confid;
    Placement placement;
};

std::mutex key_config_info_lock;
std::map<std::string, std::pair<Configuration, Configuration>> key_config_info;

string ask(const string& key){ // respond with (ready_conf_id, ready_placement, toret_conf_id, toret_placement)
    DPRINTF(DEBUG_METADATA_SERVER, "ask() key: %s\n", key.c_str());
   
    assert(key_config_info.find(key) != key_config_info.end()); // should never happen; onus lies on Controller

    return DataTransfer::serializeMDS("OK", "", key, key_config_info[key].first.confid, key_config_info[key].first.placement, 
                                      key_config_info[key].second.confid, key_config_info[key].second.placement);
}

string update(const string& key, const string& ready_conf_id, const Placement& ready_placement,
    const string& toret_conf_id, const Placement& toret_placement){

    DPRINTF(DEBUG_METADATA_SERVER, "update() started. key: %s, ready_conf_id: %s\n", key.c_str(), ready_conf_id.c_str());
    assert(ready_conf_id != toret_conf_id);
    
    // if(key_config_info.find(key) == key_config_info.end()){ // Not found!

    // }
    // else{
        key_config_info[key].first.confid = ready_conf_id;
        key_config_info[key].first.placement = ready_placement;
        key_config_info[key].second.confid = toret_conf_id;
        key_config_info[key].second.placement = toret_placement;
    // }

    DPRINTF(DEBUG_METADATA_SERVER, "key %s with ready_conf_id %s updated to toret_conf_id %s \n", key.c_str(),
            ready_conf_id.c_str(), toret_conf_id.c_str());
    return DataTransfer::serializeMDS("OK", "configuration updated");
}

void message_handler(int connection, int portid, const std::string& recvd){
    std::string status;
    std::string msg;
    std::string key;
    std::string ready_conf_id;
    std::string toret_conf_id;
    Placement toret_plcmt;

    int result = 1;
    
    Placement ready_plcmt = DataTransfer::deserializeMDS(recvd, status, msg, key, ready_conf_id, toret_conf_id, toret_plcmt);
    
    std::string& method = status; // Method: ask/update

    std::unique_lock <std::mutex> lock(key_config_info_lock);
    if(method == "ask"){
        result = DataTransfer::sendMsg(connection, ask(key));
    }
    else if(method == "update"){
        result = DataTransfer::sendMsg(connection, update(key, ready_conf_id, ready_plcmt, toret_conf_id, toret_plcmt));
    }
    else{
        DPRINTF(DEBUG_METADATA_SERVER, "Unknown method is called\n");
        DataTransfer::sendMsg(connection, DataTransfer::serializeMDS("ERROR", "Unknown method is called"));
    }
    
    if(result != 1){
        DPRINTF(DEBUG_METADATA_SERVER, "Server Response failed\n");
        DataTransfer::sendMsg(connection, DataTransfer::serializeMDS("ERROR", "Server Response failed"));
    }
}

void server_connection(int connection, int portid){

    int yes = 1;
    int result = setsockopt(connection, IPPROTO_TCP, TCP_NODELAY, (char*) &yes, sizeof(int));
    if(result < 0){
        assert(false);
    }

    while(true){
        std::string recvd;
        int result = DataTransfer::recvMsg(connection, recvd);
        if(result != 1){
            close(connection);
            std::cout << portid << endl;
            DPRINTF(DEBUG_METADATA_SERVER, "one connection closed.\n");
            return;
        }
        if(is_warmup_message(recvd)){
            std::string temp = std::string(WARM_UP_MNEMONIC) + get_random_string();
            result = DataTransfer::sendMsg(connection, temp);
            if(result != 1){
                DataTransfer::sendMsg(connection, DataTransfer::serializeMDS("ERROR", "Server Response failed"));
            }
            continue;
        }
        message_handler(connection, portid, recvd);
    }
}

void runServer(const std::string& socket_port, const std::string& socket_ip){
    
    int sock = socket_setup(socket_port, &socket_ip);
    DPRINTF(DEBUG_METADATA_SERVER, "Alive port is %s\n", socket_port.c_str());
    
    while(1){
        int portid = stoi(socket_port);
        int new_sock = accept(sock, NULL, 0);
        std::thread cThread([new_sock, portid](){ server_connection(new_sock, portid); });
        cThread.detach();
    }
    
    close(sock);
}

int main(int argc, char** argv){

    signal(SIGPIPE, SIG_IGN);

    if(argc != 3){
        std::cout << "Enter the correct number of arguments: " << argv[0] << " <ext_ip> <port_no>" << std::endl;
        return -1;
    }
    
    runServer(argv[2], argv[1]);
    
    return 0;
}


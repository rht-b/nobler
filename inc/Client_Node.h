/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   Client_Node.h
 * Author: shahrooz
 *
 * Created on August 30, 2020, 5:37 AM
 */

#ifndef CLIENT_NODE_H
#define CLIENT_NODE_H

#include "CAS_Client.h"
#include "ABD_Client.h"

class Client_Node{
public:
    Client_Node(uint32_t id, uint32_t local_datacenter_id, uint32_t retry_attempts, uint32_t metadata_server_timeout,
            uint32_t timeout_per_request, std::vector<DC*>& datacenters);
    Client_Node(const Client_Node& orig) = delete;
    virtual ~Client_Node();
    
    int put(const std::string& key, const std::string& value);
    int get(const std::string& key, std::string& value);
    
    // There must be something on the metadata server for the key
    const std::pair<Configuration, Configuration>& get_placement(const std::string& key, const bool force_update = false);
    uint32_t get_conf_id(const std::string& key);
    
    // getters
    const uint32_t& get_id() const;

private:
    // a map from a key to its conf_id and its placement
    std::map <std::string, std::pair<Configuration, Configuration> > keys_info;
    
    ABD_Client* abd;
    CAS_Client* cas;
    
    int update_placement(const std::string& key);
};

#endif /* CLIENT_NODE_H */


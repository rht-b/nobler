/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 * File:   ABD_Server.h
 * Author: shahrooz
 *
 * Created on January 4, 2020, 11:35 PM
 */

#ifndef ABD_Server_H
#define ABD_Server_H

#include <thread>
#include <vector>
#include <string>
#include <cstdlib>
#include "Cache.h"
#include "Persistent.h"
#include <mutex>
#include "Util.h"
#include "Timestamp.h"
#include "Data_Transfer.h"


class ABD_Server{
public:
    ABD_Server(const std::shared_ptr<Cache>& cache_p, const std::shared_ptr<Persistent>& persistent_p, 
               const std::shared_ptr<std::vector<std::unique_ptr<std::mutex>>>& mu_p_vec_p);
    ABD_Server(const ABD_Server& orig) = delete;
    virtual ~ABD_Server();

    std::string get_timestamp(const std::string& key, uint32_t conf_id);
    std::string put(const std::string& key, uint32_t conf_id, const std::string& value, const std::string& timestamp);
    std::string get(const std::string& key, uint32_t conf_id);

    int init_key(const std::string& key, const uint32_t conf_id);

    std::string reconfig_query(const std::string& key, uint32_t conf_id, uint32_t new_conf_id, const std::string& new_conf_placement);
    std::string reconfig_commit(const std::string& key, const std::string& timestamp, const std::string& value, uint32_t new_conf_id);
    std::string finish_reconfig(const std::string &key, uint32_t conf_id);

private:
    strVec get_data(const std::string& key);
    int put_data(const std::string& key, const strVec& value);

    std::shared_ptr<Cache> cache_p;
    std::shared_ptr<Persistent> persistent_p;
    std::shared_ptr<std::vector<std::unique_ptr<std::mutex>>> mu_p_vec_p;
};

#endif /* ABD_Server_H */

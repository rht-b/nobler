#include <thread>
#include "Data_Server.h"
#include "Data_Transfer.h"
#include <sys/ioctl.h>
#include <unordered_set>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <fstream>
#include <netinet/tcp.h>

void message_handler(int connection, DataServer& dataserver, int portid, std::string& recvd){
    auto epoch = time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
    int result = 1;
    
    strVec data = DataTransfer::deserialize(recvd);
    std::string& method = data[0];

    if(method == "put" || method == "propagate"){
        DPRINTF(DEBUG_ABD_Server, "The method put is called. The key is %s, ts: %s, value: %s, class: %s, conf_id: %s server port is %u\n",
                data[1].c_str(), data[2].c_str(), (TRUNC_STR(data[3])).c_str(), data[4].c_str(), data[5].c_str(), portid);
        result = DataTransfer::sendMsg(connection, dataserver.put(data[1], data[2], data[3], data[4], stoul(data[5])));
    }
    else if(method == "get"){
        DPRINTF(DEBUG_ABD_Server, "The method get is called. The key is %s, class: %s, conf_id: %s server port is %u\n", data[1].c_str(),
                data[2].c_str(), data[3].c_str(), portid);
        result = DataTransfer::sendMsg(connection, dataserver.get(data[1], data[2], stoul(data[3])));
    }
    else if(method == "get_timestamp"){
        DPRINTF(DEBUG_ABD_Server, "The method get_timestamp is called. The key is %s, class: %s, conf_id: %s server port is %u\n", data[1].c_str(),
                data[2].c_str(), data[3].c_str(), portid);
        result = DataTransfer::sendMsg(connection, dataserver.get_timestamp(data[1], data[2], stoul(data[3])));
    }
    else if(method == "reconfig_query"){
        DPRINTF(DEBUG_ABD_Server, "The method reconfig_query is called. The key is %s, class: %s, conf_id: %s, new_conf_id: %s, server port is %u\n", 
                data[1].c_str(), data[2].c_str(), data[3].c_str(), data[4].c_str(), portid);
            result = DataTransfer::sendMsg(connection, dataserver.reconfig_query(data[1], data[2], stoul(data[3]), stoul(data[4]), data[5]));
    }
    else if(method == "reconfig_commit"){
        DPRINTF(DEBUG_ABD_Server, "The method reconfig_commit is called. The key is %s, ts: %s, value: %s, class: %s, new_conf_id: %s server port is %u\n", 
                data[1].c_str(), data[2].c_str(), data[3].c_str(), data[4].c_str(), data[5].c_str(), portid);
            result = DataTransfer::sendMsg(connection, dataserver.reconfig_commit(data[1], data[2], (TRUNC_STR(data[3])).c_str(), data[4], stoul(data[5])));
    }
    else if(method == "finish_reconfig"){
        DPRINTF(DEBUG_ABD_Server, "The method finish_reconfig is called. The key is %s, class: %s, conf_id: %s server port is %u\n", data[1].c_str(),
                data[2].c_str(), data[3].c_str(), portid);
            result = DataTransfer::sendMsg(connection, dataserver.finish_reconfig(data[1], data[2], stoul(data[3])));
    }
    else{
        DataTransfer::sendMsg(connection, DataTransfer::serialize({"MethodNotFound", "Unknown method is called"}));
    }

    if(result != 1){
        DataTransfer::sendMsg(connection, DataTransfer::serialize({"Failure", "Server Response failed"}));
    }

    auto epoch2 = time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
    //DPRINTF(DEBUG_ABD_Server, "exec took: %lu\n", (epoch2-epoch));
}

void server_connection(int connection, DataServer& dataserver, int portid){

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
            DPRINTF(DEBUG_ABD_Server, "one connection closed.\n");
            return;
        }
        if(is_warmup_message(recvd)){
            std::string temp = std::string(WARM_UP_MNEMONIC) + get_random_string();
            result = DataTransfer::sendMsg(connection, temp);
            if(result != 1){
                DataTransfer::sendMsg(connection, DataTransfer::serialize({"Failure", "Server Response failed"}));
            }
            continue;
        }
        message_handler(connection, dataserver, portid, recvd);
    }
}


void runServer(std::string& db_name, std::string& socket_port){
    
    DataServer* ds = new DataServer(db_name, socket_setup(socket_port));
    int portid = stoi(socket_port);
    std::cout << "Alive port " << portid << std::endl;
    while(1){
        int new_sock = accept(ds->getSocketDesc(), NULL, 0);
        // std::cout << "Received Request!  PORT:" << portid << std::endl;
        std::thread cThread([&ds, new_sock, portid](){ server_connection(new_sock, *ds, portid); });
        cThread.detach();
    }
}

void runServer(const std::string& db_name, const std::string& socket_port, const std::string& socket_ip,
        const std::string& metadata_server_ip, const std::string& metadata_server_port){
    std::cout << "runServer called" << std::endl;
    DataServer* ds = new DataServer(db_name, socket_setup(socket_port, &socket_ip), metadata_server_ip,
            metadata_server_port);
    std::cout << "db created" << std::endl;
    int portid = stoi(socket_port);
    std::cout << "Alive port " << portid << std::endl;
    while(1){
        int new_sock = accept(ds->getSocketDesc(), NULL, 0);
        std::thread cThread([&ds, new_sock, portid](){ server_connection(new_sock, *ds, portid); });
        cThread.detach();
    }
    
}

int main(int argc, char** argv){

    signal(SIGPIPE, SIG_IGN);

    std::vector <std::string> socket_port;
    std::vector <std::string> db_list;
    
    if(argc == 1){
        socket_port = {"30001", "30002", "30003", "30004", "30005", "30006", "30007", "30008", "30009"};
        db_list = {"db1.temp", "db2.temp", "db3.temp", "db4.temp", "db5.temp", "db6.temp", "db7.temp", "db8.temp",
                "db9.temp"};
        for(uint i = 0; i < socket_port.size(); i++){
            fflush(stdout);
            if(fork() == 0){
                std::setbuf(stdout, NULL);
                close(1);
                int pid = getpid();
                std::stringstream filename;
                filename << "server_" << pid << "_output.txt";
                FILE* out = fopen(filename.str().c_str(), "w");
                std::setbuf(out, NULL);
                DPRINTF(true, "started on port %s", socket_port[i].c_str());
                runServer(db_list[i], socket_port[i]);
                exit(0);
            }
        }
    }
    else if(argc == 3){
        socket_port.push_back(argv[1]);
        db_list.push_back(std::string(argv[2]) + ".temp");
//        for(uint i = 0; i < socket_port.size(); i++){
//        if(socket_port[i] == "10004" || socket_port[i] == "10005"){
//            continue;
//        }
        fflush(stdout);
        if(fork() == 0){
        
            close(1);
            int pid = getpid();
            std::stringstream filename;
            filename << "server_" << pid << "_output.txt";
            fopen(filename.str().c_str(), "w");
            runServer(db_list[0], socket_port[0]);
        }
//        }
    }
    else if(argc == 6){
        socket_port.push_back(argv[2]);
        db_list.push_back(std::string(argv[3]) + ".temp");
//        for(uint i = 0; i < socket_port.size(); i++){
//        if(socket_port[i] == "10004" || socket_port[i] == "10005"){
//            continue;
//        }
        fflush(stdout);
        if(fork() == 0){
        
            close(1);
            int pid = getpid();
            std::stringstream filename;
            filename << "server_" << pid << "_output.txt";
            fopen(filename.str().c_str(), "w");
            runServer(db_list[0], socket_port[0], argv[1], argv[4], argv[5]);
        }
//        }
    }
    else{
        std::cout << "Enter the correct number of arguments :  ./Server <port_no> <db_name>" << std::endl;
        std::cout << "Or : ./Server <ext_ip> <port_no> <db_name> <metadate_server_ip> <metadata_server_port>" <<
            std::endl;
        return 0;
    }
    
    std::string ch;
    //Enter quit to exit the thread
    while(ch != "quit"){
        std::cin >> ch;
    }
    
//    std::cout << "Waiting for all detached threads to terminate!" << std::endl;
//    std::this_thread::sleep_for(std::chrono::seconds(5));
    
    return 0;
}

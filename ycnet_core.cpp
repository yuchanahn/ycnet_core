#include "ycnet_core.hpp"

struct client_t {
    ycnet::core::endpoint_t endpoint;
    std::string name;
    int my_thread_number;
};

int main()
{
    std::vector<client_t> clients;
    
    auto server = ycnet::core::UDP_CORE(4, 1234, [&clients](const char* buf, const int len, const ycnet::core::endpoint_t ip) {
        clients.emplace_back();
        //clients.back().endpoint = ip;
        //clients.back().name = buf;
        //clients.back().my_thread_number = 0;
        
        std::cout << "recv: " << buf << std::endl;
        std::cout << "from: " << ip.to_string() << ":" << ip.port << std::endl;

        send_udp(clients[0].name.c_str(), 3, clients[0].endpoint, clients[0].my_thread_number);
    });

    if(!server.has_value()) {
        printf("server error : %s\n", server.err.c_str());
    }
    
    while(true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

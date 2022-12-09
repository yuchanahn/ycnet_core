#include "ycnet_core.hpp"

int main(int argc, char* argv[])
{
    auto server = ycnet::core::UDP_CORE(4, 1234);
    return 0;
}

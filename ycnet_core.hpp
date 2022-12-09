#pragma once
#include <algorithm>
#include <SDKDDKVer.h>
#include <tchar.h>
#include <WinSock2.h>
#include <MSWsock.h>
#include <iostream>
#include <format>
#include <functional>
#include <sstream>
#include <vector>
#include <thread>
#include <WS2tcpip.h>

#include "yc_framework/ycutil.hpp"

#pragma comment(lib, "ws2_32.lib")

#define ERR_TEXT(msg) std::format("{} Error: {}", #msg , ::GetLastError()).c_str()


namespace ycnet
{
    namespace core
    {
        inline RIO_EXTENSION_FUNCTION_TABLE g_rio;
        inline HANDLE g_h_iocp = nullptr;
        inline SOCKET g_socket;

        enum RIO_Config {
            RIO_PENDING_RECVS = 100000,
            RIO_PENDING_SENDS = 10000,
            RECV_BUFFER_SIZE = 1024,
            SEND_BUFFER_SIZE = 1024,

            SEND_BUFFER_COUNT = RIO_PENDING_SENDS * SEND_BUFFER_SIZE,
            RECV_BUFFER_COUNT = RIO_PENDING_RECVS * RECV_BUFFER_SIZE,
            
            ADDR_BUFFER_SIZE = 64,

            ADDR_BUFFER_COUNT = RIO_PENDING_RECVS * ADDR_BUFFER_SIZE,
            
            RIO_MAX_RESULTS = 1000
        };
        
        inline std::vector<std::thread> worker_threads;

        enum COMPLETION_KEY {
            CK_STOP = 0,
            CK_START = 1
        };

        enum OPERATION_TYPE {
            OP_NONE = 0,
            OP_RECV = 1,
            OP_SEND = 2
        };

        struct EXTENDED_RIO_BUF : RIO_BUF {
            OPERATION_TYPE operation;
        };
        
        inline char* allocate_buf(const DWORD size)
        {
            return static_cast<char*>(
                VirtualAllocEx(
                    GetCurrentProcess(),
                    nullptr,
                    size,
                    MEM_COMMIT | MEM_RESERVE,
                    PAGE_READWRITE));
        }


        struct rio_udp_server_controller {
            std::function<void()> kill;
        };

        struct rio_t {
            EXTENDED_RIO_BUF* send_buf;
            __int64 send_buf_index;

            /// ADDR용 RIO_BUF pointer
            EXTENDED_RIO_BUF* addr_buf;
            __int64 addr_buf_index;

            OVERLAPPED overlapped;
            RIO_NOTIFICATION_COMPLETION completion_type;

            RIO_CQ cq;
            RIO_RQ rq;

            RIO_BUFFERID send_buf_id;
            RIO_BUFFERID recv_buf_id;
            RIO_BUFFERID addr_buf_id;

            char* send_buf_ptr;
            char* recv_buf_ptr;
            char* addr_buf_ptr;
        };

        inline yc::err_opt_t<rio_udp_server_controller> UDP_CORE(
            const int thread_count,
            const u_short port
            )
        {
            worker_threads.reserve(thread_count);
            static std::vector<rio_t> rios(thread_count);

            static WSADATA data;
            if (::WSAStartup(0x202, &data)) return ERR_TEXT(WSAStartup);

            g_socket = WSASocket(AF_INET, SOCK_DGRAM, IPPROTO_UDP, nullptr, 0, WSA_FLAG_REGISTERED_IO);
            if (g_socket == INVALID_SOCKET) return ERR_TEXT(WSASocket);

            static sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port = htons(port);
            addr.sin_addr.s_addr = INADDR_ANY;

            if (SOCKET_ERROR == ::bind(g_socket, reinterpret_cast<sockaddr*>(&addr), sizeof addr)) return ERR_TEXT(Bind);

            static GUID function_table_id = WSAID_MULTIPLE_RIO;
            static DWORD dw_bytes = 0;

            if (0 != WSAIoctl(g_socket,
                              SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER,
                              &function_table_id,
                              sizeof(GUID),
                              &g_rio,
                              sizeof g_rio,
                              &dw_bytes,
                              nullptr,
                              nullptr)) return ERR_TEXT(WSAIoctl);
    
            g_h_iocp = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, 0);
            if (nullptr == g_h_iocp) 
                return ERR_TEXT(CreateIoCompletionPort);
    
            int thread_number = 0;

            for (auto& [
                send_buf,
                send_buf_index,
                addr_buf,
                addr_buf_index,
                overlapped,
                completion_type,
                cq,
                rq,
                send_buf_id,
                recv_buf_id,
                addr_buf_id,
                send_buf_ptr,
                recv_buf_ptr,
                addr_buf_ptr
                ] : rios) {
        
                completion_type.Type = RIO_IOCP_COMPLETION;
                completion_type.Iocp.IocpHandle = g_h_iocp;
                completion_type.Iocp.CompletionKey = reinterpret_cast<void*>(CK_START);
                completion_type.Iocp.Overlapped = &overlapped;

                /// RIO CQ 생성 (RQ 사이즈보다 크거나 같아야 함)
                cq = g_rio.RIOCreateCompletionQueue(RIO_PENDING_RECVS + RIO_PENDING_SENDS, &completion_type);
                if (cq == RIO_INVALID_CQ) return ERR_TEXT(RIOCreateCompletionQueue);
        
                /// RIO RQ 생성
                /// SEND CQ와 RECV CQ를 같이 씀.. (따로 만들어 써도 됨)
                rq = g_rio.RIOCreateRequestQueue(
                    g_socket,
                    RIO_PENDING_RECVS,
                    1,
                    RIO_PENDING_SENDS,
                    1,
                    cq,
                    cq,
                    nullptr);
        
                if (rq == RIO_INVALID_RQ) return ERR_TEXT(RIOCreateRequestQueue);

                /// SEND용 RIO 버퍼 등록
                {
                    send_buf_ptr = allocate_buf(SEND_BUFFER_COUNT);
            
                    send_buf_id = g_rio.RIORegisterBuffer(send_buf_ptr, SEND_BUFFER_SIZE * RIO_PENDING_SENDS);

                    if (send_buf_id == RIO_INVALID_BUFFERID) return ERR_TEXT(RIORegisterBuffer);
            
                    DWORD offset = 0;

                    send_buf = new EXTENDED_RIO_BUF[SEND_BUFFER_COUNT];

                    for (DWORD j = 0; j < SEND_BUFFER_COUNT; ++j) {
                        EXTENDED_RIO_BUF* p_buffer = send_buf + j;

                        p_buffer->operation = OP_SEND;
                        p_buffer->BufferId = send_buf_id;
                        p_buffer->Offset = offset;
                        p_buffer->Length = SEND_BUFFER_SIZE;

                        offset += SEND_BUFFER_SIZE;
                    }
                }

                addr_buf_ptr = allocate_buf(ADDR_BUFFER_COUNT);
                addr_buf_id = g_rio.RIORegisterBuffer(addr_buf_ptr, ADDR_BUFFER_COUNT);

                if (addr_buf_id == RIO_INVALID_BUFFERID) return ERR_TEXT(RIORegisterBuffer);

                DWORD offset = 0;
                addr_buf = new EXTENDED_RIO_BUF[ADDR_BUFFER_COUNT];

                for (DWORD j = 0; j < ADDR_BUFFER_COUNT; ++j) {
                    EXTENDED_RIO_BUF* p_buffer = addr_buf + j;

                    p_buffer->operation = OP_NONE;
                    p_buffer->BufferId = addr_buf_id;
                    p_buffer->Offset = offset;
                    p_buffer->Length = ADDR_BUFFER_SIZE;

                    offset += ADDR_BUFFER_SIZE;
                }
        
                /// RECV용 RIO 버퍼 등록 및 RECV 미리 걸어놓기
                recv_buf_ptr = allocate_buf(RECV_BUFFER_COUNT);

                recv_buf_id = g_rio.RIORegisterBuffer(recv_buf_ptr, RECV_BUFFER_COUNT);
                if (recv_buf_id == RIO_INVALID_BUFFERID) return ERR_TEXT(RIORegisterBuffer);
        
                offset = 0;

                const auto buffs = new EXTENDED_RIO_BUF[RECV_BUFFER_COUNT];

                for (DWORD j = 0; j < RECV_BUFFER_COUNT; ++j) {
                    EXTENDED_RIO_BUF* buffer_ptr = buffs + j;

                    buffer_ptr->operation = OP_RECV;
                    buffer_ptr->BufferId = recv_buf_id;
                    buffer_ptr->Offset = offset;
                    buffer_ptr->Length = RECV_BUFFER_SIZE;

                    offset += RECV_BUFFER_SIZE;

                    if (!g_rio.RIOReceiveEx(
                        rq,
                        buffer_ptr,
                        1,
                        nullptr,
                        &addr_buf[addr_buf_index++],
                        nullptr,
                        nullptr,
                        0,
                        buffer_ptr)) return ERR_TEXT(RIOReceive);
                }

                //cout << totalBufferCount << " total receives pending" << endl;
                ++thread_number;
        
                /// IO 쓰레드 생성
                worker_threads.emplace_back([
                    &cq,
                    &rq,
                    &recv_buf_ptr,
                    &send_buf_ptr,
                    &addr_buf,
                    &send_buf,
                    &addr_buf_index,
                    &send_buf_index,
                    thread_number]
                {
            
                    DWORD number_of_bytes = 0;
                    ULONG_PTR completion_key = 0;
                    OVERLAPPED* overlapped_ptr = nullptr;

                    while (true) {
                        RIORESULT results[RIO_MAX_RESULTS];
                        if (!::GetQueuedCompletionStatus(g_h_iocp, &number_of_bytes, &completion_key, &overlapped_ptr, INFINITE))
                            return ERR_TEXT(GetQueuedCompletionStatus);

                        /// ck로 0이 넘어오면 끝낸다
                        if (completion_key == CK_STOP) break;

                        memset(results, 0, sizeof(results));

                        const ULONG num_results = g_rio.RIODequeueCompletion(cq, results, RIO_MAX_RESULTS);
                        if (0 == num_results || RIO_CORRUPT_CQ == num_results) return ERR_TEXT(RIODequeueCompletion);

                        if (const INT notify_result = g_rio.RIONotify(cq); notify_result != ERROR_SUCCESS) return ERR_TEXT(RIONotify);
                
                        for (DWORD i = 0; i < num_results; ++i) {
                            if (const auto buffer_ptr = reinterpret_cast<EXTENDED_RIO_BUF*>(results[i].RequestContext);
                                OP_RECV == buffer_ptr->operation) {
                                /// UDP니까 송신자측에서 보낸 데이터가 전부 안오는 경우는 에러
                                if (results[i].BytesTransferred != RECV_BUFFER_SIZE) break;

                                ///// ECHO TEST
                                const char* source = recv_buf_ptr + buffer_ptr->Offset;

                                EXTENDED_RIO_BUF* context = &send_buf[send_buf_index];

                                send_buf_index = (send_buf_index + 1) % SEND_BUFFER_COUNT;
                                
                                char* send_offset = send_buf_ptr + context->Offset;
                                memcpy_s(send_offset, RECV_BUFFER_SIZE, source, buffer_ptr->Length);

                                /// TEST PRINT
                                //cout << strlen(sendOffset) << " ";

                                if (!g_rio.RIOSendEx(
                                    rq,
                                    context,
                                    1,
                                    nullptr,
                                    &addr_buf[addr_buf_index % ADDR_BUFFER_COUNT],
                                    nullptr,
                                    nullptr,
                                    0,
                                    context)) return ERR_TEXT(RIOSend);
                            }
                            else if (OP_SEND == buffer_ptr->operation) {
                                if (!g_rio.RIOReceiveEx(
                                    rq,
                                    buffer_ptr,
                                    1,
                                    nullptr,
                                    &addr_buf[addr_buf_index],
                                    nullptr,
                                    nullptr,
                                    0,
                                    buffer_ptr)) return ERR_TEXT(RIOReceive);
                                addr_buf_index = (addr_buf_index + 1) % ADDR_BUFFER_COUNT;
                            }
                            else break;
                        }
                    }
                    std::stringstream ss;
                    ss << std::this_thread::get_id();
                    std::string str = ss.str();
                    
                    return std::format(" -- thread[{} : id-{}] exit -- ", thread_number, str).c_str();
                });

                if (const INT notify_result = g_rio.RIONotify(cq); notify_result != ERROR_SUCCESS) return ERR_TEXT(RIONotify);
                }

            return yc::err_opt_t(rio_udp_server_controller {
                [] {
                    if (0 == ::PostQueuedCompletionStatus(g_h_iocp, 0, CK_STOP, nullptr)) return ERR_TEXT(PostQueuedCompletionStatus);
            
                    std::ranges::for_each(worker_threads, [](std::thread& t) { t.join(); });
            
                    if (SOCKET_ERROR == ::closesocket(g_socket)) return ERR_TEXT(closesocket); 
            
                    for (const auto& rio : rios) {
                        g_rio.RIOCloseCompletionQueue(rio.cq);
                        g_rio.RIODeregisterBuffer(rio.send_buf_id);
                        g_rio.RIODeregisterBuffer(rio.recv_buf_id);
                        g_rio.RIODeregisterBuffer(rio.addr_buf_id);
                    }

                    return "SUCCESS!";
                }
            });
        }
    }
}
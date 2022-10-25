#include <iostream>

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <time.h>
#include <string>

#include <rdma/rdma_cma.h>

#include <iostream>
#include <chrono>
#include <cmath>

using namespace std;

enum
{
    RESOLVE_TIMEOUT_MS = 5000,
};

struct pdata
{
    uint64_t buf_va;
    uint32_t buf_rkey;
};

/*declaration*/

struct pdata server_pdata;
struct rdma_event_channel *cm_channel;
struct rdma_cm_id *cm_id;
struct rdma_cm_event *event;
struct rdma_conn_param conn_param = {};
struct ibv_pd *pd;
struct ibv_comp_channel *comp_chan;
struct ibv_cq *cq;
struct ibv_cq *evt_cq;
struct ibv_mr *mr;
struct ibv_qp_init_attr qp_attr = {};
struct ibv_sge sge;
struct ibv_send_wr send_wr = {};
struct ibv_send_wr *bad_send_wr;
struct ibv_recv_wr recv_wr = {};
struct ibv_recv_wr *bad_recv_wr;
struct ibv_wc wc;
void *cq_context;
struct addrinfo *res, *t;
struct addrinfo hints = {

    .ai_family = AF_INET,
    .ai_socktype = SOCK_STREAM

};

int n;
char *buf;
int err;

chrono::system_clock::time_point StartTime;
chrono::system_clock::time_point EndTime;

int pre_conn(char *argv)
{
    /*create event channel*/

    cm_channel = rdma_create_event_channel();

    if (!cm_channel)
        return 1;

    err = rdma_create_id(cm_channel, &cm_id, NULL, RDMA_PS_TCP);

    if (err)
        return err;

    n = getaddrinfo(argv, "20069", &hints, &res);

    if (n < 0)
        return 1;

    /*resolve the address and route*/

    for (t = res; t; t = t->ai_next)
    {

        err = rdma_resolve_addr(cm_id, NULL, t->ai_addr, RESOLVE_TIMEOUT_MS);

        if (!err)
            break;
    }

    if (err)
        return err;

    err = rdma_get_cm_event(cm_channel, &event);

    if (err)
        return err;

    if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED)
        return 1;

    rdma_ack_cm_event(event);

    err = rdma_resolve_route(cm_id, RESOLVE_TIMEOUT_MS);

    if (err)
        return err;

    err = rdma_get_cm_event(cm_channel, &event);

    if (err)
        return err;

    if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED)
        return 1;

    rdma_ack_cm_event(event);

    /*allocate protection domain*/

    pd = ibv_alloc_pd(cm_id->verbs);

    if (!pd)
        return 1;

    comp_chan = ibv_create_comp_channel(cm_id->verbs);

    if (!comp_chan)
        return 1;

    cq = ibv_create_cq(cm_id->verbs, 2, NULL, comp_chan, 0);

    if (!cq)
        return 1;

    if (ibv_req_notify_cq(cq, 0))
        return 1;

    if (!buf)
        return 1;

    mr = ibv_reg_mr(pd, buf, 10000 * sizeof(char), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);

    if (!mr)
        return 1;

    qp_attr.cap.max_send_wr = 1;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_wr = 1;
    qp_attr.cap.max_recv_sge = 1;
    qp_attr.send_cq = cq;
    qp_attr.recv_cq = cq;
    qp_attr.qp_type = IBV_QPT_RC;

    err = rdma_create_qp(cm_id, pd, &qp_attr);

    if (err)
        return err;

    conn_param.initiator_depth = 1;
    conn_param.retry_count = 7;

    return 0; /*added*/
}

/*connecting to server*/

int conn_send_data()
{
    err = rdma_connect(cm_id, &conn_param);

    if (err)
        return err;

    err = rdma_get_cm_event(cm_channel, &event);

    if (err)
        return err;

    if (event->event != RDMA_CM_EVENT_ESTABLISHED)
        return 1;

    memcpy(&server_pdata, event->param.conn.private_data, sizeof server_pdata);

    rdma_ack_cm_event(event);

    /* Prepost*/

    sge.addr = (uintptr_t)buf;
    sge.length = 10000 * sizeof(char);
    sge.lkey = mr->lkey;
    recv_wr.wr_id = 0;
    recv_wr.sg_list = &sge;
    recv_wr.num_sge = 1;

    if (ibv_post_recv(cm_id->qp, &recv_wr, &bad_recv_wr))
        return 1;


   // buf = htonl(buf);
    printf("** Send Data : %s\n",buf);
    sge.addr = (uintptr_t)buf;
    sge.length = 10000 * sizeof(char);
    sge.lkey = mr->lkey;
    send_wr.wr_id = 1;
    send_wr.opcode = IBV_WR_SEND;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.wr.rdma.rkey = ntohl(server_pdata.buf_rkey);
    send_wr.wr.rdma.remote_addr = ntohl(server_pdata.buf_va);

    StartTime = chrono::system_clock::now();
    err = ibv_post_send(cm_id->qp, &send_wr, &bad_send_wr);
    chrono::system_clock::time_point EndTime = chrono::system_clock::now();
    chrono::duration<double> DefaultSec = EndTime - StartTime;
    cout << "Time : " << DefaultSec.count() << endl;

   // if (ibv_post_send(cm_id->qp, &send_wr, &bad_send_wr))

    if (err)
        return 1;
    else {
        return 0;
    }

}

/*receive reply from server*/

int recv_answer()
{
    printf("** Received Data : ");

    while (1)
    {

        if (ibv_get_cq_event(comp_chan, &evt_cq, &cq_context))
            return 1;

        if (ibv_req_notify_cq(cq, 0))
            return 1;

        if (ibv_poll_cq(cq, 1, &wc) != 1)
            return 1;

        if (wc.status != IBV_WC_SUCCESS)
            return 1;

        if (wc.wr_id == 0)
        {
            //for (int i = 0; i < 10; i++)
            printf("%s ", buf);

            printf("\n");
            return 0;
        }
    }

    return 0;
}

int main(int argc, char *argv[])
{

    char *arr;
    int err;

    arr = (char *)malloc(10000 * sizeof(char));

    strcpy(arr, "{\"queryID\":3,\"workID\":1,\"tableName\":\"lineitem\",\"tableCol\":[\"l_orderkey\",\"l_partkey\",\"l_suppkey\",\"l_linenumber\",\"l_quantity\",\"l_extendedprice\",\"l_discount\",\"l_tax\",\"l_returnflag\",\"l_linestatus\",\"l_shipdate\",\"l_commitdate\",\"l_receiptdate\",\"l_shipinstruct\",\"l_shipmode\",\"l_comment\"],\"tableFilter\":[{\"LV\":\"l_commitdate\",\"OPERATOR\":4,\"RV\":\"l_receiptdate\"}],\"columnFiltering\":[\"l_orderkey\"],\"groupBy\":[],\"Order_By\":[],\"columnProjection\":[{\"selectType\":0,\"value\":[\"l_orderkey\"],\"valueType\":[10]}],\"tableOffset\":[0,4,8,12,16,23,30,37,44,45,46,49,52,55,80,90],\"tableOfflen\":[4,4,4,4,7,7,7,7,1,1,3,3,3,25,10,44],\"tableDatatype\":[3,3,3,3,246,246,246,246,254,254,14,14,14,254,254,15],\"projectionDatatype\":[3],\"blockList\":[{\"offset\":142610432,\"length\":[4041,4037,4047,3960,4066,4053,4066,4054,4004,4011,3954,4052,4048,4023,3982,4038,4006,3957,4030,4020,3998,4065,4035,4059,4007,3978,4084,3972,4060,3986,3967,3997,4027,3956,4066,4082,4072,4039,3977,4048,3974,4057,4032,4078,4018,4057,3994,4043,4069,4078,4077,3958,4008,4037,4056,4057,4077,3994,4032,3986,4071,4053,3994,4003,3959,4085,4040,4074,4008,4045,4052,4072,4018,3955,3960,4068,4005,3987,4047,4083,4076,4009,4078,4065,4082,4068,4081,3948,4087,4011,3999,4074,3969,4061,3967,4039,4073,3962,4048,3993,4056,4031,4087,4085,4055,4060,4064,3969,4071,4004,4043,3973,4081,4039,3992,4060,3987,3973,4041,4046,3961,4087,4044,3989,3959,4044,4084,4054,4053,4054,3980,3992,4000,4087,4020,4066,3977,4051,4038,3998,4024,4046,3963,3973,3988,4004,4084,4053,4026,3945,4083,3954,4045,4061,3990,4086,4022,4051,3998,4023,4083,4018,3996,3951,3988,4022,3999,4047,4077,4087,3974,4078,4076,3996,4042,4053,3974,3968,4022,3978,4074,4000,3995,4064,4077,4057,4013,4038,3970,3988,4066,3971,4017,4024,4083,4050,3996,4030,4021,4010,4031,4065,4021,4073,4001,3960,4074,4012,4044,4024,4016,4003,4003,4029,3999,3976,4020,4004,4019,3987,4072,4002,3966,3974,3966,3950,4041,4065,4035,3988,4076,4061,4050,4013,3973,3990,3977,3975,4031,4052,3989,3985,3999,4047,946]}],\"primaryKey\":0,\"csdName\":\"8\"}");

    buf = arr;
    err = pre_conn(argv[1]);

    if (!err)
    {
        err = conn_send_data();
        if (!err)
        {
           err = recv_answer();
        }
    }
}

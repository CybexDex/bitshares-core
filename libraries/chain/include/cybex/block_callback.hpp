#pragma once
#include <graphene/chain/protocol/base.hpp>
#include <graphene/chain/database.hpp>
namespace graphene {
namespace chain {


class block_callback
{
public:
    block_callback() {}
    void handler(database & db);
private:
   void process_crowdfund(database & db) const ;
   void auto_withdraw(database & db,const crowdfund_object & crowdfund) const;
   void crowdfund_ended(database & db,const crowdfund_object & crowdfund) const;
};




} }

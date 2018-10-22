#pragma once

#include <graphene/app/plugin.hpp>
#include <graphene/chain/database.hpp>
#include <graphene/db/generic_index.hpp>

#include <boost/multi_index/composite_key.hpp>

namespace graphene { namespace limit_order_status {

using namespace chain;

#ifndef LIMIT_ORDER_STATUS_SPACE_ID
#define LIMIT_ORDER_STATUS_SPACE_ID 7
#endif

enum limit_order_status_object_type
{
    limit_order_status_object_type = 0
};

struct status_key
{
    status_key( asset_id_type a1, asset_id_type a2 ): asset1(a1), asset2(a2) {}
    status_key() {}

    friend bool operator < ( const status_key& a, const status_key& b)
    {
        if( a.asset1 != b.asset1 )
            return a.asset1 < b.asset1;
        return a.asset2 < b.asset2;
    }
    friend bool operator == ( const status_key& a, const status_key& b)
    {
        return std::tie(a.asset1, a.asset2) == std::tie(b.asset1, b.asset2); 
    }
    asset_id_type   asset1;
    asset_id_type   asset2;
};

struct limit_order_status_object : public abstract_object<limit_order_status_object>
{
    static const uint8_t space_id = LIMIT_ORDER_STATUS_SPACE_ID;
    static const uint8_t type_id  = limit_order_status_object_type;

    limit_order_id_type order_id;
    account_id_type     seller;
    status_key          key;
    bool                is_sell; /* true means sell asset1, false means receive asset1 */
    share_type          amount_to_sell;
    share_type          min_to_receive;
    share_type          sold;
    share_type          received;
    share_type          canceled;
    uint32_t            block_num;
    uint16_t            trx_in_blk;
    uint16_t            op_in_trx;
    fc::time_point_sec  create_time;

    inline void on_trade(share_type pays, share_type receives)
    {
        sold += pays;
        received += receives;
    }

    inline void on_cancel()
    {
        assert(canceled == 0);
        canceled = amount_to_sell - sold;
        assert(canceled > 0);
    }

    inline bool is_opened() const
    {
        if( canceled > 0 )
            return false;

        if(amount_to_sell != sold)
            return true;

        return false;
    }
};

struct by_order_id;
struct by_status_key;
struct by_open_time;
struct by_seller;

typedef multi_index_container<
    limit_order_status_object,
    indexed_by<
        ordered_unique< tag<by_id>, member< object, object_id_type, &object::id > >,
        ordered_unique< tag<by_order_id>,
                        member< limit_order_status_object, 
                                limit_order_id_type,
                                &limit_order_status_object::order_id > >,
        ordered_unique<
            tag<by_seller>,
            composite_key<
                limit_order_status_object,
                member< limit_order_status_object, account_id_type, &limit_order_status_object::seller >,
                const_mem_fun< limit_order_status_object, bool, &limit_order_status_object::is_opened >,
                member< limit_order_status_object, limit_order_id_type, &limit_order_status_object::order_id >
            >,
            composite_key_compare<
                std::less< account_id_type >,
                std::less< bool >,
                std::greater< limit_order_id_type >
            >
        >,
        ordered_unique<
            tag<by_status_key>,
            composite_key<
                limit_order_status_object,
                member< limit_order_status_object, account_id_type, &limit_order_status_object::seller >,
                member< limit_order_status_object, status_key, &limit_order_status_object::key >,
                const_mem_fun< limit_order_status_object, bool, &limit_order_status_object::is_opened >,
                member< limit_order_status_object, limit_order_id_type, &limit_order_status_object::order_id >
            >,
            composite_key_compare<
                std::less< account_id_type >,
                std::less< status_key >,
                std::less< bool >,
                std::greater< limit_order_id_type >
            >
        >,
        ordered_unique<
            tag<by_open_time>,
            composite_key<
                limit_order_status_object,
                member< limit_order_status_object, fc::time_point_sec, &limit_order_status_object::create_time >,
                member< object, object_id_type, &object::id >
            >
        >
    >
> limit_order_status_multi_index_type;

typedef generic_index<limit_order_status_object, limit_order_status_multi_index_type> limit_order_status_index;

namespace detail
{
    class limit_order_status_plugin_impl;    
}

class limit_order_status_plugin : public graphene::app::plugin
{
    public:
        limit_order_status_plugin();
        virtual ~limit_order_status_plugin();

        std::string plugin_name() const override;
        virtual void plugin_set_program_options(
            boost::program_options::options_description& cli,
            boost::program_options::options_description& cfg) override;
        virtual void plugin_initialize(
            const boost::program_options::variables_map& options) override;
        virtual void plugin_startup() override;

    private:
        friend class detail::limit_order_status_plugin_impl;
        std::unique_ptr<detail::limit_order_status_plugin_impl> my;
};

}}

FC_REFLECT( graphene::limit_order_status::status_key, (asset1)(asset2) )
FC_REFLECT_DERIVED( graphene::limit_order_status::limit_order_status_object, (graphene::db::object),
                    (order_id)
                    (seller)
                    (key)
                    (is_sell)
                    (amount_to_sell)
                    (min_to_receive)
                    (sold)
                    (received)
                    (canceled)
                    (block_num)
                    (trx_in_blk)
                    (op_in_trx)
                    (create_time) )

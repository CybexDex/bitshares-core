#include <graphene/limit_order_status/limit_order_status_plugin.hpp>

#include <graphene/chain/account_object.hpp>
#include <graphene/chain/config.hpp>
#include <graphene/chain/database.hpp>
#include <graphene/chain/operation_history_object.hpp>
#include <graphene/limit_order_status/ignore_accounts.hpp>

#include <boost/filesystem/path.hpp>

namespace graphene { namespace limit_order_status {

using namespace chain;

namespace detail {
class limit_order_status_plugin_impl{
    public:
        limit_order_status_plugin_impl(limit_order_status_plugin& _plugin)
            : _self(_plugin){}
        virtual ~limit_order_status_plugin_impl();

        graphene::chain::database& database()
        {
            return _self.database();
        }

        void update_limit_order_status( const signed_block& b );
        bool ignore_account( const account_id_type& a );

        limit_order_status_plugin&      _self;
        uint32_t                        _order_status_timeout_sec = 86400;
        ignore_accounts                 _ignore;
};

limit_order_status_plugin_impl::~limit_order_status_plugin_impl()
{
    return;
}

struct limit_order_status_visitor
{
    limit_order_status_plugin_impl&     _impl;
    fc::time_point_sec                  _now;
    const operation_history_object&     _oho;

    typedef void result_type;

    limit_order_status_visitor(
        limit_order_status_plugin_impl& i,
        fc::time_point_sec n,
        const operation_history_object& o
        ):
        _impl(i), _now(n), _oho(o){}

    template<typename T>
    void operator()( const T& o) const {}

    void operator()( const graphene::chain::limit_order_create_operation& o) const
    {
        if( _impl.ignore_account(o.seller) )
            return;

        status_key key(o.amount_to_sell.asset_id, o.min_to_receive.asset_id);
        bool is_sell = true;

        if(key.asset1 > key.asset2)
        {
            std::swap(key.asset1, key.asset2);
            is_sell = false;
        }
        auto& db = _impl.database();
        db.create<limit_order_status_object>(
            [&]( limit_order_status_object& so ){
                so.order_id = _oho.result.get<object_id_type>();
                so.seller = o.seller;
                so.key = key;
                so.is_sell = is_sell;
                so.amount_to_sell = o.amount_to_sell.amount;
                so.min_to_receive = o.min_to_receive.amount;
                so.sold = 0;
                so.received = 0;
                so.canceled = 0;
                so.block_num = _oho.block_num;
                so.trx_in_blk = _oho.trx_in_block;
                so.op_in_trx = _oho.op_in_trx;
                so.create_time = _now;
            });
    }

    void operator()( const graphene::chain::fill_order_operation& o) const
    {
        if( _impl.ignore_account(o.account_id) )
            return;

        limit_order_id_type order_id;
        try {
            order_id = o.order_id.as<graphene::chain::limit_order_id_type>();
        } catch( ... ) {
            /* if order id is not valid type, skip */
            return;
        };

        auto & db = _impl.database();
        auto & idx = db.get_index_type<limit_order_status_index>().indices().get<by_order_id>();
        auto itr = idx.find(order_id);

        if( itr == idx.end() ) /* order filled may have been timed out in this plugin */
            return;
        
        db.modify( *itr,
            [&](limit_order_status_object& so){
                so.on_trade(o.pays.amount, o.receives.amount);
            });
    }

    void operator()( const graphene::chain::limit_order_cancel_operation& o) const
    {
        if( _impl.ignore_account(o.fee_paying_account) )
            return;

        if(o.extensions.size() > 0)
            /* TODO: if rte raises vop of limit order cancel, we can just return,
                     else keep tract of map between transaction id and order id.
               TODO: John said he will consider add vop of limit order cancel by trx id.
             */
            return;

        auto & db = _impl.database();
        auto & idx = db.get_index_type<limit_order_status_index>().indices().get<by_order_id>();
        auto itr = idx.find(o.order);
        
        if( itr == idx.end() )
        {/* the order canceled by user has been removed due to expiration */
            return;
        }
        db.modify( *itr,
            [&](limit_order_status_object& so){
                so.on_cancel();
            });
    }
};

void limit_order_status_plugin_impl::update_limit_order_status( const signed_block& b )
{
    graphene::chain::database& db = database();
    const vector<optional< operation_history_object > >& hist = db.get_applied_operations();

    for( const optional< operation_history_object >& oho : hist )
    {
        if( oho.valid() )
        {
            try {
                oho->op.visit( limit_order_status_visitor( *this, b.timestamp, *oho ) );
            } FC_CAPTURE_AND_LOG( (oho) );
        }
    }

    /* clear expired */
    fc::time_point_sec earliest_to_keep = b.timestamp - _order_status_timeout_sec;
    const auto& limit_order_status_idx = db.get_index_type<limit_order_status_index>().indices().get<by_open_time>();
    //uint32_t remove_cnt = 0;
    while( !limit_order_status_idx.empty() &&
            limit_order_status_idx.begin()->create_time < earliest_to_keep )
    {
        const limit_order_status_object& status = *limit_order_status_idx.begin();
        db.remove(status);
    //    remove_cnt++;
    }
    //if(remove_cnt > 0)
    //    ilog("remove ${r}", ("r", remove_cnt));
}

bool limit_order_status_plugin_impl::ignore_account( const account_id_type& a )
{
    return _ignore.is_ignored(a);
}

}

limit_order_status_plugin::limit_order_status_plugin():
    my( new detail::limit_order_status_plugin_impl(*this) )
{ }

limit_order_status_plugin::~limit_order_status_plugin()
{ }

std::string limit_order_status_plugin::plugin_name() const
{
    return "limit_order_status";
}

void limit_order_status_plugin::plugin_set_program_options(
    boost::program_options::options_description& cli,
    boost::program_options::options_description& cfg)
{
    cli.add_options()
        ("ignore-accounts", boost::program_options::value<boost::filesystem::path>(), "JSON file of ignore account id" )
        ("order-status-timeout-sec", boost::program_options::value<uint32_t>(), "Timeout seconds of order status to be kept in memory")
    ;
    cfg.add(cli);
}

void limit_order_status_plugin::plugin_initialize(const boost::program_options::variables_map& options)
{
    database().applied_block.connect( [this](const signed_block& b){ my->update_limit_order_status(b); } );
    database().add_index< primary_index< limit_order_status_index > >(); 

    if( options.count("order-status-timeout-sec") )
        my->_order_status_timeout_sec = options["order-status-timeout-sec"].as<uint32_t>();
    if( options.count("ignore-accounts") ) {
        if( fc::exists(options.at("ignore-accounts").as<boost::filesystem::path>()) )
        {
            my->_ignore = fc::json::from_file(options.at("ignore-accounts").as<boost::filesystem::path>() ).as<ignore_accounts>( 2 );
            ilog( "Using ignore account from file ${p}",
                  ("p", options.at("ignore-accounts").as<boost::filesystem::path>().string() )
                );
        }
        else
        {
            elog( "Failed to load file from ${p}",
                 ("p", options.at("ignore-accounts").as<boost::filesystem::path>().string()) );
            std::exit(EXIT_FAILURE);
        }
    }

    ilog("Limit order status plugin initialized with time out ${s} seconds, ${i} ignore accounts",
         ("s", my->_order_status_timeout_sec)("i", my->_ignore.ignores.size()));
}

void limit_order_status_plugin::plugin_startup()
{
    ilog("Start limit order status plugin with timeout ${s} seconds", ("s", my->_order_status_timeout_sec));
}

}}

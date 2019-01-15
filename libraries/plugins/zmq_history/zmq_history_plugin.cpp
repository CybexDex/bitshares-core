/*
 * Copyright (c) 2017 Cryptonomex, Inc., and contributors.
 *
 * The MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#include <iostream>
#include <graphene/zmq_history/zmq_history_plugin.hpp>

#include <zmq.hpp>


#include <graphene/app/impacted.hpp>

#include <graphene/chain/account_evaluator.hpp>
#include <graphene/chain/account_object.hpp>
#include <graphene/chain/config.hpp>
#include <graphene/chain/database.hpp>
#include <graphene/chain/evaluator.hpp>
#include <graphene/chain/operation_history_object.hpp>
#include <graphene/chain/transaction_evaluation_state.hpp>

#include <fc/smart_ref_impl.hpp>
#include <fc/thread/thread.hpp>
#include <fc/io/json.hpp>
#include <fc/utf8.hpp>
#include <fc/static_variant.hpp>
#include <fc/variant.hpp>

#include <boost/chrono.hpp>
#include <boost/signals2/connection.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/find.hpp>
#include <boost/algorithm/string.hpp>
#include <regex>


#include <atomic>
#include <queue>

namespace graphene { namespace zmq_history {

namespace detail
{

class zmq_history_plugin_impl
{
   public:
      zmq_history_plugin_impl(zmq_history_plugin& _plugin)
         : _self( _plugin )
      {  
      }
      // zmq_history_plugin_impl();
      ~zmq_history_plugin_impl();

      void update_account_histories( const signed_block& b );

      graphene::chain::database& database()
      {
         return _self.database();
      }

      zmq_history_plugin& _self;
      primary_index< operation_history_index >* _oho_index;

//      vector <string> bulk; //  vector of op lines
	std::string zmq_history_addr = "ipc://zmq_history.ipc";
      void init();
	zmq::context_t * context;
	zmq::socket_t * publisher;

      bool configured{false};
      uint32_t start_block_num = 0;
      bool not_begin_until= false;
      bool start_block_reached = false;
      bool _zmq_history_visitor = false;

      std::string db_name;

      size_t queue_size = 0;
      boost::mutex mtx;
      // boost::thread consume_thread;
      std::atomic<bool> startup{false};


   private:
      void add_zmq_history( const account_id_type account_id, const optional<operation_history_object>& oho, const signed_block& b );
      void processBulkLine(account_transaction_history_object ath, operation_history_struct os, int op_type, block_struct bs, visitor_struct vs);

};



void handle_zmq_exception( const std::string& desc, int line_num ) {
   bool shutdown = true;
   try {
      try {
         throw;
      } catch(  zmq::error_t & e) {
         // logic_error on invalid key, do not shutdown
         wlog( "logic error, ${desc}, line ${line}, code ${code}, ${what}",
               ("desc", desc)( "line", line_num )( "code",zmq_errno() )( "what", e.what() ));
         shutdown = false;
      }catch( fc::exception& er ) {
         elog( "fc exception, ${desc}, line ${line}, ${details}",
               ("desc", desc)( "line", line_num )( "details", er.to_detail_string()));
      } catch( const std::exception& e ) {
         elog( "std exception, ${desc}, line ${line}, ${what}",
               ("desc", desc)( "line", line_num )( "what", e.what()));
      } catch( ... ) {
         elog( "unknown exception, ${desc}, line ${line_nun}", ("desc", desc)( "line_num", line_num ));
      }
   } catch (...) {
      std::cerr << "Exception attempting to handle exception for " << desc << " " << line_num << std::endl;
   }

   if( shutdown ) {
      // shutdown if failed to provide opportunity to fix issue and restart
      // app().quit();
   }
}

zmq_history_plugin_impl::~zmq_history_plugin_impl()
{
	if(startup){
		delete publisher;
		delete context;
	}
   return;
}


void zmq_history_plugin_impl::init() {
   // Create the native contract accounts manually; sadly, we can't run their contracts to make them create themselves
   // See native_contract_chain_initializer::prepare_database()

   try {
      // blocks indexes
  } catch(...) {
      handle_zmq_exception("create indexes", __LINE__);
   }
   // ilog("starting db plugin thread");
   // consume_thread = boost::thread([this] { consume_blocks(); });

   startup = true;
}

void zmq_history_plugin_impl::update_account_histories( const signed_block& b )
{
   graphene::chain::database& db = database();
   const vector<optional< operation_history_object > >& hist = db.get_applied_operations();
   bool is_first = true;
   auto skip_oho_id = [&is_first,&db,this]() {
      if( is_first && db._undo_db.enabled() ) // this ensures that the current id is rolled back on undo
      {
         db.remove( db.create<operation_history_object>( []( operation_history_object& obj) {} ) );
         is_first = false;
      }
      else
         _oho_index->use_next_id();
   };
   for( const optional< operation_history_object >& o_op : hist ) {
      optional <operation_history_object> oho;

      auto create_oho = [&]() {
         is_first = false;
         return optional<operation_history_object>(
               db.create<operation_history_object>([&](operation_history_object &h) {
                  if (o_op.valid())
                  {
                     h.op           = o_op->op;
                     h.result       = o_op->result;
                     h.block_num    = o_op->block_num;
                     h.trx_in_block = o_op->trx_in_block;
                     h.op_in_trx    = o_op->op_in_trx;
                     h.virtual_op   = o_op->virtual_op;
                  }
               }));
      };

      if( !o_op.valid() ) {
         skip_oho_id();
         continue;
      }
      oho = create_oho();

      const operation_history_object& op = *o_op;

      // get the set of accounts this operation applies to
      flat_set<account_id_type> impacted;
      vector<authority> other;
      operation_get_required_authorities( op.op, impacted, impacted, other ); // fee_payer is added here

      if( op.op.which() == operation::tag< account_create_operation >::value )
         impacted.insert( op.result.get<object_id_type>() );
      else
         graphene::app::operation_get_impacted_accounts( op.op, impacted );

      for( auto& a : other )
         for( auto& item : a.account_auths )
            impacted.insert( item.first );

      for( auto& account_id : impacted )
      {
         add_zmq_history( account_id, oho, b );
      }
   }
}

void zmq_history_plugin_impl::add_zmq_history( const account_id_type account_id, const optional <operation_history_object>& oho, const signed_block& b)
{
   graphene::chain::database& db = database();
   const auto &stats_obj = account_id(db).statistics(db);

   // add new entry
   const auto &ath = db.create<account_transaction_history_object>([&](account_transaction_history_object &obj) {
      obj.operation_id = oho->id;
      obj.account = account_id;
      obj.sequence = stats_obj.total_ops + 1;
      obj.next = stats_obj.most_recent_op;
   });

   // keep stats growing as no op will be removed
   db.modify(stats_obj, [&](account_statistics_object &obj) {
      obj.most_recent_op = ath.id;
      obj.total_ops = ath.sequence;
   });

   // operation_type
   int op_type = -1;
   if (!oho->id.is_null())
      op_type = oho->op.which();

   // operation history data
   operation_history_struct os;
   os.trx_in_block = oho->trx_in_block;
   os.op_in_trx = oho->op_in_trx;
   os.operation_result = fc::json::to_string(oho->result);
   os.virtual_op = oho->virtual_op;
   os.op = fc::json::to_string(oho->op);

   // visitor data
   visitor_struct vs;
   if(_zmq_history_visitor) {
      operation_visitor o_v;
      oho->op.visit(o_v);

      vs.fee_data.asset = o_v.fee_asset;
      vs.fee_data.amount = o_v.fee_amount;
      vs.transfer_data.asset = o_v.transfer_asset_id;
      vs.transfer_data.amount = o_v.transfer_amount;
      vs.transfer_data.from = o_v.transfer_from;
      vs.transfer_data.to = o_v.transfer_to;
   }

   // block data
   std::string trx_id = "";
   if(!b.transactions.empty() && oho->trx_in_block < b.transactions.size()) {
      trx_id = b.transactions[oho->trx_in_block].id().str();
   }
   block_struct bs;
   bs.block_num = b.block_num();
   bs.block_time = b.timestamp;
   bs.trx_id = trx_id;

   // check if we are in replay or in sync and change number of bulk documents accordingly
   // uint32_t limit_documents = 0;
   // if((fc::time_point::now() - b.timestamp) < fc::seconds(30))
   //    limit_documents = _zmq_history_bulk_sync;
   // else
   //    limit_documents = _zmq_history_bulk_replay;
   if(!not_begin_until && (int)start_block_num > bs.block_num){
      // ilog("Now processBulkLine is disabled!");
   }else {
      processBulkLine(ath, os, op_type, bs, vs); // we have everything, creating bulk line
   }

   // remove everything except current object from ath
   const auto &his_idx = db.get_index_type<account_transaction_history_index>();
   const auto &by_seq_idx = his_idx.indices().get<by_seq>();
   auto itr = by_seq_idx.lower_bound(boost::make_tuple(account_id, 0));
   if (itr != by_seq_idx.end() && itr->account == account_id && itr->id != ath.id) {
      // if found, remove the entry
      const auto remove_op_id = itr->operation_id;
      const auto itr_remove = itr;
      ++itr;
      db.remove( *itr_remove );
      // modify previous node's next pointer
      // this should be always true, but just have a check here
      if( itr != by_seq_idx.end() && itr->account == account_id )
      {
         db.modify( *itr, [&]( account_transaction_history_object& obj ){
            obj.next = account_transaction_history_id_type();
         });
      }
      // do the same on oho
      const auto &by_opid_idx = his_idx.indices().get<by_opid>();
      if (by_opid_idx.find(remove_op_id) == by_opid_idx.end()) {
         db.remove(remove_op_id(db));
      }
   }
}

void zmq_history_plugin_impl::processBulkLine(account_transaction_history_object ath, operation_history_struct os, int op_type, block_struct bs, visitor_struct vs)
{
   bulk_struct bulk_;
   bulk_.account_history = ath;
   bulk_.operation_type = op_type;
   bulk_.block_data = bs;

   std::string index_name = "account_history";
   std::string _id = fc::json::to_string(ath.id);

   std::string opstring(std::begin(os.op) + os.op.find_first_of("{") , std::end(os.op) - 1);
   pub_obj_struct obj;
   obj.bulk = bulk_;
   obj.op = opstring;
   obj.result = os.operation_result;
   try {
	auto obj_string = "history " + fc::json::to_string( obj );	
	try {
                zmq::message_t message(obj_string.size());
                memcpy(message.data(), obj_string.data(), obj_string.size());
                publisher->send(message);

   	} catch(...) {
      		handle_zmq_exception("failed to insert: " + opstring , __LINE__);
   	}

   } catch(...){
      elog( "  JSON: ${j}", ("j", opstring ) ) ;
      handle_zmq_exception("error when construct : " + opstring , __LINE__);
   }
}

} // end namespace detail

zmq_history_plugin::zmq_history_plugin() :   my( new detail::zmq_history_plugin_impl(*this) )
{
}

zmq_history_plugin::~zmq_history_plugin()
{
}

std::string zmq_history_plugin::plugin_name()const
{
   return "zmq_history";
}
std::string zmq_history_plugin::plugin_description()const
{
   return "Stores account history data in zmq_history database(EXPERIMENTAL).";
}

void zmq_history_plugin::plugin_set_program_options(
   boost::program_options::options_description& cli,
   boost::program_options::options_description& cfg
   )
{
   namespace bpo = boost::program_options;
   cli.add_options()
         ("zmq_history-queue-size,q", bpo::value<uint32_t>()->default_value(256),
         "The target queue size between nodeos and MongoDB plugin thread.")
         ("zmq_history-block-start", bpo::value<uint32_t>()->default_value(0),
         "If specified then only abi data pushed to zmq_history until specified block is reached.")
         ("zmq_history-addr", bpo::value<std::string>(),
         "MongoDB URI connection string, see: https://docs.zmq_history.com/master/reference/connection-string/."
               " If not specified then plugin is disabled. Default database 'cybex' is used if not specified in URI."
               " Example: zmq_history://127.0.0.1:27017/cybex")
         ;
   cfg.add(cli);
}

void zmq_history_plugin::plugin_initialize(const boost::program_options::variables_map& options)
{
   database().applied_block.connect( [&]( const signed_block& b){ my->update_account_histories(b); } );
   my->_oho_index = database().add_index< primary_index< operation_history_index > >();
   database().add_index< primary_index< account_transaction_history_index > >();
   try {
      if( options.count( "zmq_history-addr" )) {
         ilog( "initializing zmq_history_plugin..." );
	 my->zmq_history_addr =  options.at( "zmq_history-addr" ).as<string>();
         my->configured = true;
         if( options.count( "replay-blockchain" ) || options.count( "resync-blockchain" ) ) {
            ilog("replay/resync blockchain");
         }
         if( options.count( "zmq_history-queue-size" )) {
            my->queue_size = options.at( "zmq_history-queue-size" ).as<uint32_t>();
         }
         if( options.count( "zmq_history-block-start" )) {
            my->start_block_num = options.at( "zmq_history-block-start" ).as<uint32_t>();
         }
         if( my->start_block_num == 0 ) {
            my->start_block_reached = true;
         }else {
            ilog("clear zmq_history since start blk ");
            my->not_begin_until = true;
            my->start_block_reached = false;
            // to do 
         }
         ilog( "connecting to ${u}", ("u", my->zmq_history_addr));
	my->context = new zmq::context_t(1);
        my->publisher = new zmq::socket_t(*my->context , ZMQ_PUB);
        my->publisher->bind(my->zmq_history_addr.data());

         if( my->not_begin_until ) {
         }
         my->init();
      } else {
         wlog( "eosio::zmq_history_plugin configured, but no --zmq_history-addr specified." );
         wlog( "zmq_history_plugin disabled." );
      }
   } FC_LOG_AND_RETHROW()
}

void zmq_history_plugin::plugin_startup()
{
}
// void zmq_history_plugin::plugin_shutdown()
// {

//    my.reset();
// }
}

} 

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
#include <graphene/mongodb/mongodb_plugin.hpp>

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

#include <curl/curl.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/find.hpp>
#include <boost/algorithm/string.hpp>
#include <regex>


#include <atomic>
#include <queue>

#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/exception/exception.hpp>
#include <bsoncxx/json.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/collection.hpp>


namespace graphene { namespace mongodb {

namespace detail
{

class mongodb_plugin_impl
{
   public:
      mongodb_plugin_impl(mongodb_plugin& _plugin)
         : _self( _plugin ),mongo_inst{}, mongo_conn{}
      {  
         // curl = curl_easy_init(); 
      }
      // mongodb_plugin_impl();
      ~mongodb_plugin_impl();

      void update_account_histories( const signed_block& b );

      graphene::chain::database& database()
      {
         return _self.database();
      }

      mongodb_plugin& _self;
      primary_index< operation_history_index >* _oho_index;

      vector <string> bulk; //  vector of op lines
      void init();
      void clear_database_since(uint32_t clear_num);
      void wipe_database();

      bool configured{false};
      bool wipe_database_on_startup{false};
      bool reset_mongo_index{false};
      bool clear_database_on_startup{false};
      uint32_t start_block_num = 0;
      bool start_block_reached = false;
      bool _mongodb_visitor = false;

      std::string db_name;
      mongocxx::instance mongo_inst;
      mongocxx::client mongo_conn;
      mongocxx::collection accounts;

      size_t queue_size = 0;
      boost::mutex mtx;
      boost::condition_variable condition;
      // boost::thread consume_thread;
      std::atomic<bool> done{false};
      std::atomic<bool> startup{true};


   private:
      void add_mongodb( const account_id_type account_id, const optional<operation_history_object>& oho, const signed_block& b );
      void processBulkLine(account_transaction_history_object ath, operation_history_struct os, int op_type, block_struct bs, visitor_struct vs);
      void sendBulk(std::string _mongodb_node_url, bool _mongodb_logs);

};

namespace {

template<typename Queue, typename Entry>
void queue(boost::mutex& mtx, boost::condition_variable& condition, Queue& queue, const Entry& e, size_t queue_size) {
   int sleep_time = 100;
   size_t last_queue_size = 0;
   boost::mutex::scoped_lock lock(mtx);
   if (queue.size() > queue_size) {
      lock.unlock();
      condition.notify_one();
      if (last_queue_size < queue.size()) {
         sleep_time += 100;
      } else {
         sleep_time -= 100;
         if (sleep_time < 0) sleep_time = 100;
      }
      last_queue_size = queue.size();
      boost::this_thread::sleep_for(boost::chrono::milliseconds(sleep_time));
      lock.lock();
   }
   queue.emplace_back(e);
   lock.unlock();
   condition.notify_one();
}

}


void handle_mongo_exception( const std::string& desc, int line_num ) {
   bool shutdown = true;
   try {
      try {
         throw;
      } catch( mongocxx::logic_error& e) {
         // logic_error on invalid key, do not shutdown
         wlog( "mongo logic error, ${desc}, line ${line}, code ${code}, ${what}",
               ("desc", desc)( "line", line_num )( "code", e.code().value() )( "what", e.what() ));
         shutdown = false;
      } catch( mongocxx::operation_exception& e) {
         elog( "mongo exception, ${desc}, line ${line}, code ${code}, ${details}",
               ("desc", desc)( "line", line_num )( "code", e.code().value() )( "details", e.code().message() ));
         if (e.raw_server_error()) {
            elog( "mongo exception, ${desc}, line ${line}, ${details}",
                  ("desc", desc)( "line", line_num )( "details", bsoncxx::to_json(e.raw_server_error()->view())));
         }
      } catch( mongocxx::exception& e) {
         elog( "mongo exception, ${desc}, line ${line}, code ${code}, ${what}",
               ("desc", desc)( "line", line_num )( "code", e.code().value() )( "what", e.what() ));
      } catch( bsoncxx::exception& e) {
         elog( "bsoncxx exception, ${desc}, line ${line}, code ${code}, ${what}",
               ("desc", desc)( "line", line_num )( "code", e.code().value() )( "what", e.what() ));
      } catch( fc::exception& er ) {
         elog( "mongo fc exception, ${desc}, line ${line}, ${details}",
               ("desc", desc)( "line", line_num )( "details", er.to_detail_string()));
      } catch( const std::exception& e ) {
         elog( "mongo std exception, ${desc}, line ${line}, ${what}",
               ("desc", desc)( "line", line_num )( "what", e.what()));
      } catch( ... ) {
         elog( "mongo unknown exception, ${desc}, line ${line_nun}", ("desc", desc)( "line_num", line_num ));
      }
   } catch (...) {
      std::cerr << "Exception attempting to handle exception for " << desc << " " << line_num << std::endl;
   }

   if( shutdown ) {
      // shutdown if mongo failed to provide opportunity to fix issue and restart
      // app().quit();
   }
}

// mongodb_plugin_impl::mongodb_plugin_impl()
// : mongo_inst{}
// , mongo_conn{}
// {
// }

mongodb_plugin_impl::~mongodb_plugin_impl()
{
   return;
}

void mongodb_plugin_impl::clear_database_since(uint32_t clear_num) {
   ilog("mongo db clear_database_since");
   using namespace bsoncxx::types;
   using namespace bsoncxx::builder::stream;
   auto account_history = mongo_conn[db_name]["account_history"];
   // std::stringstream ss;
   // ss << clear_num;
   // std::string clear_num_str = ss.str();
   // auto json_str = "{\"block_data.block_num\" : { $gte : " + clear_num_str + "} }";
   // // auto json_str = "{\"block_data.block_num\" : " + clear_num_str + "} ";
   // ilog(json_str);
   // const auto& value = bsoncxx::from_json( json_str );
   auto query_value = document{} << "bulk.block_data.block_num" << open_document << "$gte" << b_int64{clear_num} << close_document << finalize;
   try{
      // auto col_doc = bsoncxx::builder::basic::document{};
      // col_doc.append( bsoncxx::builder::concatenate_doc{value.view()} );
      account_history.delete_many( query_value.view());
   }catch(...){
      // elog(json_str);
      handle_mongo_exception("delete_many ", __LINE__);
   }
}
void mongodb_plugin_impl::wipe_database() {
   ilog("mongo db wipe_database");

   auto account_history = mongo_conn[db_name]["account_history"];
   // auto blocks = mongo_conn[db_name][blocks_col];
   // accounts = mongo_conn[db_name][accounts_col];

   account_history.drop();
}

void mongodb_plugin_impl::init() {

   try {
      // blocks indexes
      auto account_history = mongo_conn[db_name]["account_history"]; // Blocks
      // account_history.create_index( bsoncxx::from_json( R"xxx({ "account_history.id" : 1 })xxx" ));
      if(reset_mongo_index == true){
      	
        account_history.indexes().drop_all();
      }
      if(wipe_database_on_startup == true || reset_mongo_index == true){
        // account_history.create_index( bsoncxx::from_json( R"xxx({ "bulk.account_history.account" : 1 })xxx" ));
        account_history.create_index( bsoncxx::from_json( R"xxx({ "bulk.block_data.block_num" : -1 })xxx" ));
        account_history.create_index( bsoncxx::from_json( R"xxx({ "bulk.account_history.operation_id" : 1 })xxx" ));
        account_history.create_index( bsoncxx::from_json( R"xxx({ "bulk.block_data.trx_id" : 1 })xxx" ));
        account_history.create_index( bsoncxx::from_json( R"xxx({ "bulk.operation_type" : 1 })xxx" ));
        account_history.create_index( bsoncxx::from_json( R"xxx({ "bulk.account_history.account" : 1, "bulk.block_data.block_time": -1 })xxx" ));
        // account_history.create_index( bsoncxx::from_json( R"xxx({ "bulk.account_history.account" : 1, "bulk.operation_type": 1 })xxx" ));
        account_history.create_index( bsoncxx::from_json( R"xxx({ "op.fee.asset_id" : 1 })xxx" ));
        account_history.create_index( bsoncxx::from_json( R"xxx({ "bulk.account_history.account" : 1, "op.amount.asset_id" : 1, "bulk.block_data.block_time": -1 })xxx" ));
        account_history.create_index( bsoncxx::from_json( R"xxx({ "bulk.account_history.account" : 1, "op.amount_to_sell.asset_id" : 1, "bulk.block_data.block_time": -1 })xxx" ));
        account_history.create_index( bsoncxx::from_json( R"xxx({ "bulk.account_history.account" : 1, "op.min_to_receive.asset_id" : 1, "bulk.block_data.block_time": -1 })xxx" ));
        account_history.create_index( bsoncxx::from_json( R"xxx({ "bulk.account_history.account" : 1, "op.pays.asset_id" : 1, "bulk.block_data.block_time": -1 })xxx" ));
        account_history.create_index( bsoncxx::from_json( R"xxx({ "bulk.account_history.account" : 1, "op.receives.asset_id" : 1, "bulk.block_data.block_time": -1 })xxx" ));
        account_history.create_index( bsoncxx::from_json( R"xxx({ "bulk.account_history.account" : 1, "op.fill_price.base.asset_id" : 1, "op.fill_price.quote.asset_id" : 1,"bulk.block_data.block_time": -1 },{"name":"fill_order_idx_1"})xxx" ));
        account_history.create_index( bsoncxx::from_json( R"xxx({ "bulk.account_history.account" : 1, "op.fill_price.quote.asset_id" : 1,"bulk.block_data.block_time": -1 },{"name":"fill_order_idx_2"})xxx" ));

        account_history.create_index( bsoncxx::from_json( R"xxx({ "op.seller" : 1, "result.1": 1 })xxx" )); // create limit order, op1
        account_history.create_index( bsoncxx::from_json( R"xxx({ "op.order" : 1 })xxx" ));// cancel order, op2 
        account_history.create_index( bsoncxx::from_json( R"xxx({ "op.order_id" : 1 })xxx" )); // fill order, op4
	account_history.create_index( bsoncxx::from_json( R"xxx({ "bulk.block_data.block_time": 1 })xxx" )); 
	account_history.create_index( bsoncxx::from_json( R"xxx({ "op.fill_price.base.asset_id": 1, "op.fill_price.quote.asset_id": 1 })xxx" )); 
	account_history.create_index( bsoncxx::from_json( R"xxx({ "op.fill_price.base.asset_id": 1, "op.fill_price.quote.asset_id": 1, "bulk.block_data.block_time": -1 })xxx" )); 
	account_history.create_index( bsoncxx::from_json( R"xxx({ "bulk.block_data.block_time": -1, "bulk.operation_type": 1 })xxx" )); 
	
      }
   } catch(...) {
      handle_mongo_exception("create indexes", __LINE__);
   }
   

   ilog("starting db plugin thread");

   // consume_thread = boost::thread([this] { consume_blocks(); });

   startup = false;
}

void mongodb_plugin_impl::update_account_histories( const signed_block& b )
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
         add_mongodb( account_id, oho, b );
      }
   }
}

void mongodb_plugin_impl::add_mongodb( const account_id_type account_id, const optional <operation_history_object>& oho, const signed_block& b)
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
   if(_mongodb_visitor) {
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
   //    limit_documents = _mongodb_bulk_sync;
   // else
   //    limit_documents = _mongodb_bulk_replay;
   if(!wipe_database_on_startup && (int)start_block_num > bs.block_num){
      // ilog("Now processBulkLine is disabled!");
   }else {
      processBulkLine(ath, os, op_type, bs, vs); // we have everything, creating bulk line
   }

   // if (curl && bulk.size() >= limit_documents) { // we are in bulk time, ready to add data to mongodb
   //    sendBulk(_mongodb_node_url, _mongodb_logs);
   // }

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

void mongodb_plugin_impl::processBulkLine(account_transaction_history_object ath, operation_history_struct os, int op_type, block_struct bs, visitor_struct vs)
{
   bulk_struct bulks;
   bulks.account_history = ath;
   // bulks.operation_history = os;
   bulks.operation_type = op_type;
   bulks.block_data = bs;
   // bulks.additional_data = vs;

   std::string alltogether = fc::json::to_string(bulks);
   // std::string op_result= os.operation_result;

   // auto block_date = bulks.block_data.block_time.to_iso_string();
   // std::vector<std::string> parts;
   // boost::split(parts, block_date, boost::is_any_of("-"));
   // std::string index_name = "graphene-" + parts[0] + "-" + parts[1];
   std::string index_name = "account_history";
   std::string _id = fc::json::to_string(ath.id);
   using namespace bsoncxx::types;
   using bsoncxx::builder::basic::kvp;

   auto col = mongo_conn[db_name][index_name];
   auto col_doc = bsoncxx::builder::basic::document{};

   auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
         std::chrono::microseconds{fc::time_point::now().time_since_epoch().count()});
   // const auto& opvalue = fc::json::variants_from_string( os.op);
   // auto opstring = fc::json::to_string(opvalue[1]);
   // std::string opstring(std::begin(os.op) + 3, std::end(os.op) - 1);
   std::string opstring(std::begin(os.op) + os.op.find_first_of("{") , std::end(os.op) - 1);
   try {
      // const auto& value = bsoncxx::from_json( alltogether );
      // col_doc.append( bsoncxx::builder::concatenate_doc{value.view()} );
      // auto opstring = fc::json::to_string(opvalue[1]);
      col_doc.append(
            kvp( "bulk", bsoncxx::from_json(alltogether).view() ),
            kvp( "op", bsoncxx::from_json(opstring).view() ),
            kvp( "result", bsoncxx::from_json(os.operation_result).view() )
        );
   } catch(...){
      elog( "  JSON: ${j}", ("j", alltogether));
      elog( "  JSON: ${j}", ("j", opstring ) ) ;
      handle_mongo_exception("error when construct : " + opstring , __LINE__);
   }
   // catch( bsoncxx::exception& ) {
   //    try {
   //       alltogether = fc::prune_invalid_utf8( alltogether );
   //       const auto& value = bsoncxx::from_json( alltogether );
   //       col_doc.append( bsoncxx::builder::concatenate_doc{value.view()} );
   //       col_doc.append( kvp( "non-utf8-purged", b_bool{true} ));
   //    } catch( bsoncxx::exception& e ) {
   //       elog( "Unable to convert transaction JSON to MongoDB JSON: ${e}", ("e", e.what()));
   //       elog( "  JSON: ${j}", ("j", alltogether));
   //    }
   // }
   // col_doc.append( kvp( "createdAt", b_date{now} ));

   try {
      if( !col.insert_one( col_doc.view())) {
         ilog( "Failed to insert ${"+ _id + "}");
      }
   } catch(...) {
      handle_mongo_exception("col insert: " + opstring , __LINE__);
   }

   // bulk header before each line, op_type = create to avoid dups, index id will be ath id(2.9.X).
   // std::string _id = fc::json::to_string(ath.id);
   // bulk.push_back("{ \"index\" : { \"_index\" : \""+index_name+"\", \"_type\" : \"data\", \"op_type\" : \"create\", \"_id\" : "+_id+" } }"); // header
   // bulk.push_back(alltogether);
}
} // end namespace detail

mongodb_plugin::mongodb_plugin() :   my( new detail::mongodb_plugin_impl(*this) )
{
}

mongodb_plugin::~mongodb_plugin()
{
}

std::string mongodb_plugin::plugin_name()const
{
   return "mongodb";
}
std::string mongodb_plugin::plugin_description()const
{
   return "Stores account history data in mongodb database(EXPERIMENTAL).";
}

void mongodb_plugin::plugin_set_program_options(
   boost::program_options::options_description& cli,
   boost::program_options::options_description& cfg
   )
{
   namespace bpo = boost::program_options;
   cli.add_options()
         ("mongodb-queue-size,q", bpo::value<uint32_t>()->default_value(256),
         "The target queue size between nodeos and MongoDB plugin thread.")
         ("reset-mongodb-index", bpo::bool_switch()->default_value(false),
         "Reset mongo indexes, drop then create new indexes.")
	 ("mongodb-wipe", bpo::bool_switch()->default_value(false),
         "Required with --replay-blockchain, --resync-blockchain to wipe mongo db."
         "This option required to prevent accidental wipe of mongo db.")
         ("mongodb-block-start", bpo::value<uint32_t>()->default_value(0),
         "If specified then only abi data pushed to mongodb until specified block is reached.")
         ("mongodb-uri,m", bpo::value<std::string>(),
         "MongoDB URI connection string, see: https://docs.mongodb.com/master/reference/connection-string/."
               " If not specified then plugin is disabled. Default database 'cybex' is used if not specified in URI."
               " Example: mongodb://127.0.0.1:27017/cybex")
         ;
   cfg.add(cli);
}

void mongodb_plugin::plugin_initialize(const boost::program_options::variables_map& options)
{
   database().applied_block.connect( [&]( const signed_block& b){ my->update_account_histories(b); } );
   my->_oho_index = database().add_index< primary_index< operation_history_index > >();
   database().add_index< primary_index< account_transaction_history_index > >();
   try {
      if( options.count( "mongodb-uri" )) {
         ilog( "initializing mongo_db_plugin" );
         my->configured = true;
         // if(options.count( "replay-blockchain" )) ilog("replay > 0");
         // if(options["resync-blockchain"].as<bool>()) ilog("resync > 0");
         if( options.count( "replay-blockchain" ) || options.count( "resync-blockchain" ) ) {
            ilog("replay/resync blockchain");
            if( options.count( "mongodb-wipe" ) && options.at( "mongodb-wipe" ).as<bool>()) {
               ilog( "Wiping mongo database on startup" );
               my->wipe_database_on_startup = true;
            } else if( options.count( "mongodb-block-start" ) == 0 ) {
               ilog( "--mongodb-wipe required with --replay-blockchain, --resync-blockchain. --mongodb-wipe will remove all cybex collections from mongodb." );
            }
         }
	 if( options.count("reset-mongodb-index")  && options.at( "reset-mongodb-index" ).as<bool>() ){
		ilog("Reseting mongo index");
		my->reset_mongo_index = true;
	 }
         if( options.count( "mongodb-queue-size" )) {
            my->queue_size = options.at( "mongodb-queue-size" ).as<uint32_t>();
         }
         if( options.count( "mongodb-block-start" )) {
            my->start_block_num = options.at( "mongodb-block-start" ).as<uint32_t>();
         }
         if( my->start_block_num == 0 ) {
            my->start_block_reached = true;
         }else {
            ilog("clear mongodb since start blk ");
            my->clear_database_on_startup = true;
            // to do 
         }
         std::string uri_str = options.at( "mongodb-uri" ).as<std::string>();
         ilog( "connecting to ${u}", ("u", uri_str));
         mongocxx::uri uri = mongocxx::uri{uri_str};
         my->db_name = uri.database();
         if( my->db_name.empty())
            my->db_name = "cybexops";
         my->mongo_conn = mongocxx::client{uri};

         // hook up to signals on controller
         // chain_plugin* chain_plug = app().find_plugin<chain_plugin>();
         // auto& chain = chain_plug->chain();
         // my->chain_id.emplace( chain.get_chain_id());

         

         if( my->wipe_database_on_startup ) {
            my->wipe_database();
         }
         if( my->clear_database_on_startup ) {
            my->clear_database_since(my->start_block_num);
         }
         my->init();
      } else {
         wlog( "eosio::mongo_db_plugin configured, but no --mongodb-uri specified." );
         wlog( "mongo_db_plugin disabled." );
      }
   } FC_LOG_AND_RETHROW()
}

void mongodb_plugin::plugin_startup()
{
}
// void mongodb_plugin::plugin_shutdown()
// {

//    my.reset();
// }
}

} 

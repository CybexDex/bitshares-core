/*
 * Copyright (c) 2018 oxarbitrage, and contributors.
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

#include <graphene/zmq_objects/zmq_objects.hpp>

#include <fc/smart_ref_impl.hpp>

#include <curl/curl.h>
#include <graphene/chain/proposal_object.hpp>
#include <graphene/chain/balance_object.hpp>
#include <graphene/chain/market_object.hpp>



#include <zmq.hpp>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>


namespace graphene { namespace zmq_objects {

namespace detail
{

class zmq_objects_plugin_impl
{
   public:
      zmq_objects_plugin_impl(zmq_objects_plugin& _plugin)
         : _self( _plugin )
      {  
	// curl = curl_easy_init(); 
	
      }
      virtual ~zmq_objects_plugin_impl();

      void updateDatabase( const vector<object_id_type>& ids , bool isNew);

      zmq_objects_plugin& _self;
      std::string _zmq_objects_zmq_addr = "ipc://zmqobj.ipc";
      bool start = false;
      zmq::context_t * context;
      zmq::socket_t * publisher;

      uint32_t _zmq_objects_bulk_replay = 1;
      uint32_t _zmq_objects_bulk_sync = 1;
      bool _zmq_objects_proposals = false;
      bool _zmq_objects_accounts = false;
      bool _zmq_objects_assets = false;
      bool _zmq_objects_balances = false;
      bool _zmq_objects_limit_orders = false;
      bool _zmq_objects_asset_bitasset = false;
      bool _zmq_objects_logs = true;
      CURL *curl; // curl handler
      vector <std::string> bulk;
      map<object_id_type, bitasset_struct> bitassets;
      map<object_id_type, account_struct> accounts;
      map<object_id_type, proposal_struct> proposals;
      map<object_id_type, asset_struct> assets;
      map<object_id_type, balance_struct> balances;
      map<object_id_type, limit_order_struct> limit_orders;
      //uint32_t bitasset_seq;
   private:
      void PrepareProposal(const proposal_object* proposal_object, const fc::time_point_sec block_time, uint32_t block_number);
      void PrepareAccount(const account_object* account_object, const fc::time_point_sec block_time, uint32_t block_number);
      void PrepareAsset(const asset_object* asset_object, const fc::time_point_sec block_time, uint32_t block_number);
      void PrepareBalance(const balance_object* balance_object, const fc::time_point_sec block_time, uint32_t block_number);
      void PrepareLimit(const limit_order_object* limit_object, const fc::time_point_sec block_time, uint32_t block_number);
      void PrepareBitAsset(const asset_bitasset_data_object* bitasset_object, const fc::time_point_sec block_time, uint32_t block_number);
};


void zmq_objects_plugin_impl::updateDatabase( const vector<object_id_type>& ids , bool isNew)
{
   graphene::chain::database &db = _self.database();

   const fc::time_point_sec block_time = db.head_block_time();
   const uint32_t block_number = db.head_block_num();

   // check if we are in replay or in sync and change number of bulk documents accordingly
   uint32_t limit_documents = 0;
   if((fc::time_point::now() - block_time) < fc::seconds(30))
      limit_documents = _zmq_objects_bulk_sync;
   else
      limit_documents = _zmq_objects_bulk_replay;

   if (bulk.size() >= limit_documents) { 
	for(auto b : bulk){
		zmq::message_t message(b.size());
		memcpy(message.data(), b.data(), b.size());
		publisher->send(message);
	}
     	bulk.clear();
   }

   for(auto const& value: ids) {
      if(value.is<proposal_object>() && _zmq_objects_proposals) {
         auto obj = db.find_object(value);
         auto p = static_cast<const proposal_object*>(obj);
         if(p != nullptr)
            PrepareProposal(p, block_time, block_number);
      }
      else if(value.is<account_object>() && _zmq_objects_accounts) {
         auto obj = db.find_object(value);
         auto a = static_cast<const account_object*>(obj);
         if(a != nullptr)
            PrepareAccount(a, block_time, block_number);
      }
      else if(value.is<asset_object>() && _zmq_objects_assets) {
         auto obj = db.find_object(value);
         auto a = static_cast<const asset_object*>(obj);
         if(a != nullptr)
            PrepareAsset(a, block_time, block_number);
      }
      else if(value.is<balance_object>() && _zmq_objects_balances) {
	ilog("balance_object");
         auto obj = db.find_object(value);
         auto b = static_cast<const balance_object*>(obj);
         if(b != nullptr)
            PrepareBalance(b, block_time, block_number);
      }
      else if(value.is<limit_order_object>() && _zmq_objects_limit_orders) {
         auto obj = db.find_object(value);
         auto l = static_cast<const limit_order_object*>(obj);
         if(l != nullptr)
            PrepareLimit(l, block_time, block_number);
      }
      else if(value.is<asset_bitasset_data_object>() && _zmq_objects_asset_bitasset) {
         auto obj = db.find_object(value);
         auto ba = static_cast<const asset_bitasset_data_object*>(obj);
         if(ba != nullptr)
            PrepareBitAsset(ba, block_time, block_number);
      }
   }
}

void zmq_objects_plugin_impl::PrepareProposal(const proposal_object* proposal_object, const fc::time_point_sec block_time, uint32_t block_number)
{
   proposal_struct prop;
   prop.object_id = proposal_object->id;
   prop.block_time = block_time;
   prop.block_number = block_number;
   prop.expiration_time = proposal_object->expiration_time;
   prop.review_period_time = proposal_object->review_period_time;
   prop.proposed_transaction = fc::json::to_string(proposal_object->proposed_transaction);
   prop.required_owner_approvals = fc::json::to_string(proposal_object->required_owner_approvals);
   prop.available_owner_approvals = fc::json::to_string(proposal_object->available_owner_approvals);
   prop.required_active_approvals = fc::json::to_string(proposal_object->required_active_approvals);
   prop.available_key_approvals = fc::json::to_string(proposal_object->available_key_approvals);
   prop.proposer = proposal_object->proposer;

   auto it = proposals.find(proposal_object->id);
   if(it == proposals.end())
      proposals[proposal_object->id] = prop;
   else {
      if(it->second == prop) return;
      else proposals[proposal_object->id] = prop;
   }

   std::string data = "bitsharzmq-proposal " + fc::json::to_string(prop);
   bulk.push_back(data);
}

void zmq_objects_plugin_impl::PrepareAccount(const account_object* account_object, const fc::time_point_sec block_time, uint32_t block_number)
{
   account_struct acct;
   acct.object_id = account_object->id;
   acct.block_time = block_time;
   acct.block_number = block_number;
   acct.membership_expiration_date = account_object->membership_expiration_date;
   acct.registrar = account_object->registrar;
   acct.referrer = account_object->referrer;
   acct.lifetime_referrer = account_object->lifetime_referrer;
   acct.network_fee_percentage = account_object->network_fee_percentage;
   acct.lifetime_referrer_fee_percentage = account_object->lifetime_referrer_fee_percentage;
   acct.referrer_rewards_percentage = account_object->referrer_rewards_percentage;
   acct.name = account_object->name;
   acct.owner_account_auths = fc::json::to_string(account_object->owner.account_auths);
   acct.owner_key_auths = fc::json::to_string(account_object->owner.key_auths);
   acct.owner_address_auths = fc::json::to_string(account_object->owner.address_auths);
   acct.active_account_auths = fc::json::to_string(account_object->active.account_auths);
   acct.active_key_auths = fc::json::to_string(account_object->active.key_auths);
   acct.active_address_auths = fc::json::to_string(account_object->active.address_auths);
   acct.voting_account = account_object->options.voting_account;

   auto it = accounts.find(account_object->id);
   if(it == accounts.end())
      accounts[account_object->id] = acct;
   else {
      if(it->second == acct) return;
      else accounts[account_object->id] = acct;
   }

   std::string data = "bitsharzmq-account " + fc::json::to_string(acct);
   bulk.push_back(data);
}

void zmq_objects_plugin_impl::PrepareAsset(const asset_object* asset_object, const fc::time_point_sec block_time, uint32_t block_number)
{
   asset_struct _asset;
   _asset.object_id = asset_object->id;
   _asset.block_time = block_time;
   _asset.block_number = block_number;
   _asset.symbol = asset_object->symbol;
   _asset.issuer = asset_object->issuer;
   _asset.is_market_issued = asset_object->is_market_issued();
   _asset.dynamic_asset_data_id = asset_object->dynamic_asset_data_id;
   _asset.bitasset_data_id = asset_object->bitasset_data_id;

   auto it = assets.find(asset_object->id);
   if(it == assets.end())
      assets[asset_object->id] = _asset;
   else {
      if(it->second == _asset) return;
      else assets[asset_object->id] = _asset;
   }

   std::string data = "bitsharzmq-asset " + fc::json::to_string(_asset);
   bulk.push_back(data);
}

void zmq_objects_plugin_impl::PrepareBalance(const balance_object* balance_object, const fc::time_point_sec block_time, uint32_t block_number)
{
   balance_struct balance;
   balance.object_id = balance_object->id;
   balance.block_time = block_time;
   balance.block_number = block_number;
   balance.owner = balance_object->owner;
   balance.account= balance_object->sender;
   balance.asset_id = balance_object->balance.asset_id;
   balance.amount = balance_object->balance.amount;

   auto it = balances.find(balance_object->id);
   if(it == balances.end())
      balances[balance_object->id] = balance;
   else {
      if(it->second == balance) return;
      else balances[balance_object->id] = balance;
   }

   std::string data = "bitsharzmq-balance " + fc::json::to_string(balance);
   bulk.push_back(data);
}

void zmq_objects_plugin_impl::PrepareLimit(const limit_order_object* limit_object, const fc::time_point_sec block_time, uint32_t block_number)
{
   limit_order_struct limit;
   limit.object_id = limit_object->id;
   limit.block_time = block_time;
   limit.block_number = block_number;
   limit.expiration = limit_object->expiration;
   limit.seller = limit_object->seller;
   limit.for_sale = limit_object->for_sale;
   limit.sell_price = limit_object->sell_price;
   limit.deferred_fee = limit_object->deferred_fee;

   auto it = limit_orders.find(limit_object->id);
   if(it == limit_orders.end())
      limit_orders[limit_object->id] = limit;
   else {
      if(it->second == limit) return;
      else limit_orders[limit_object->id] = limit;
   }

   std::string data = "bitsharzmq-limitorder " + fc::json::to_string(limit);
   bulk.push_back(data);
}

void zmq_objects_plugin_impl::PrepareBitAsset(const asset_bitasset_data_object* bitasset_object, const fc::time_point_sec block_time, uint32_t block_number)
{
   if(!bitasset_object->is_prediction_market) {

      bitasset_struct bitasset;
      bitasset.object_id = bitasset_object->id;
      bitasset.block_time = block_time;
      bitasset.block_number = block_number;
      bitasset.current_feed = fc::json::to_string(bitasset_object->current_feed);
      bitasset.current_feed_publication_time = bitasset_object->current_feed_publication_time;

      auto it = bitassets.find(bitasset_object->id);
      if(it == bitassets.end())
         bitassets[bitasset_object->id] = bitasset;
      else {
         if(it->second == bitasset) return;
         else bitassets[bitasset_object->id] = bitasset;
      }

      std::string data = "bitsharzmq-bitasset " + fc::json::to_string(bitasset);
      bulk.push_back(data);
   }
}

zmq_objects_plugin_impl::~zmq_objects_plugin_impl()
{
	if(start){
		delete publisher;
		delete context;
	}
	return;
}


} // end namespace detail

zmq_objects_plugin::zmq_objects_plugin() :
   my( new detail::zmq_objects_plugin_impl(*this) )
{
}

zmq_objects_plugin::~zmq_objects_plugin()
{
}

std::string zmq_objects_plugin::plugin_name()const
{
   return "zmq_objects";
}
std::string zmq_objects_plugin::plugin_description()const
{
   return "Stores blockchain objects in ES database. Experimental.";
}

void zmq_objects_plugin::plugin_set_program_options(
   boost::program_options::options_description& cli,
   boost::program_options::options_description& cfg
   )
{
   cli.add_options()
         ("zmq-objects-zmq-addr", boost::program_options::value<std::string>(), "zmq addr")
         ("zmq-objects-logs", boost::program_options::value<bool>(), "Log bulk events to database")
         ("zmq-objects-bulk-replay", boost::program_options::value<uint32_t>(), "Number of bulk documents to index on replay(5000)")
         ("zmq-objects-bulk-sync", boost::program_options::value<uint32_t>(), "Number of bulk documents to index on a syncronied chain(10)")
         ("zmq-objects-proposals", boost::program_options::value<bool>(), "Store proposal objects")
         ("zmq-objects-accounts", boost::program_options::value<bool>(), "Store account objects")
         ("zmq-objects-assets", boost::program_options::value<bool>(), "Store asset objects")
         ("zmq-objects-balances", boost::program_options::value<bool>(), "Store balances objects")
         ("zmq-objects-limit-orders", boost::program_options::value<bool>(), "Store limit order objects")
         ("zmq-objects-asset-bitasset", boost::program_options::value<bool>(), "Store feed data")

         ;
   cfg.add(cli);
}

void zmq_objects_plugin::plugin_initialize(const boost::program_options::variables_map& options)
{
   database().new_objects.connect([&]( const vector<object_id_type>& ids, const flat_set<account_id_type>& impacted_accounts ){ my->updateDatabase(ids, 1); });
   database().changed_objects.connect([&]( const vector<object_id_type>& ids, const flat_set<account_id_type>& impacted_accounts ){ my->updateDatabase(ids, 0); });

   if (options.count("zmq-objects-zmq-addr")) {
      my->_zmq_objects_zmq_addr = options["zmq-objects-zmq-addr"].as<std::string>();
   }
   if (options.count("zmq-objects-logs")) {
      my->_zmq_objects_logs = options["zmq-objects-logs"].as<bool>();
   }
   if (options.count("zmq-objects-bulk-replay")) {
      my->_zmq_objects_bulk_replay = options["zmq-objects-bulk-replay"].as<uint32_t>();
   }
   if (options.count("zmq-objects-bulk-sync")) {
      my->_zmq_objects_bulk_sync = options["zmq-objects-bulk-sync"].as<uint32_t>();
   }
   if (options.count("zmq-objects-proposals")) {
	ilog("enable proposals!");
      my->_zmq_objects_proposals = options["zmq-objects-proposals"].as<bool>();
   }
   if (options.count("zmq-objects-accounts")) {
	ilog("enable accounts!");
      my->_zmq_objects_accounts = options["zmq-objects-accounts"].as<bool>();
   }
   if (options.count("zmq-objects-assets")) {
	ilog("enable assets!");
      my->_zmq_objects_assets = options["zmq-objects-assets"].as<bool>();
   }
   if (options.count("zmq-objects-balances")) {
	ilog("enable balances!");
      my->_zmq_objects_balances = options["zmq-objects-balances"].as<bool>();
   }
   if (options.count("zmq-objects-limit-orders")) {
	ilog("enable limit-orders!");
      my->_zmq_objects_limit_orders = options["zmq-objects-limit-orders"].as<bool>();
   }
   if (options.count("zmq-objects-asset-bitasset")) {
	ilog("enable asset-bitasset!");
      my->_zmq_objects_asset_bitasset = options["zmq-objects-asset-bitasset"].as<bool>();
   }
   if( !(my->_zmq_objects_proposals || my->_zmq_objects_accounts || my->_zmq_objects_assets || my->_zmq_objects_balances || my->_zmq_objects_limit_orders) ){
	elog("no objects enabled!");
	std::exit(EXIT_FAILURE);
   }
	ilog("connecting to " + my->_zmq_objects_zmq_addr+ " ...");
	my->context = new zmq::context_t(1);
        my->publisher = new zmq::socket_t(*my->context , ZMQ_PUB);
        my->publisher->bind(my->_zmq_objects_zmq_addr.data());
	my->start = true;
}

void zmq_objects_plugin::plugin_startup()
{
   ilog("zmq objects: plugin_startup() begin");
}

} }

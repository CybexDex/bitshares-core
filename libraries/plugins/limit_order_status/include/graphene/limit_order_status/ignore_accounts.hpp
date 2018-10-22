#pragma once

#include <fc/reflect/reflect.hpp>
#include <boost/container/flat_set.hpp>

namespace graphene { namespace limit_order_status {

using boost::container::flat_set;

struct ignore_accounts
{
   flat_set<account_id_type> ignores;
   inline bool is_ignored(account_id_type a)
   {
      if(ignores.size() > 0 && ignores.find(a) != ignores.end() )
         return true;
      return false;
   }
};

} } 

FC_REFLECT( graphene::limit_order_status::ignore_accounts,
    (ignores)
   )


file(GLOB HEADERS "include/graphene/limit_order_status/*.hpp")

add_library( graphene_limit_order_status
             limit_order_status_plugin.cpp
           )

target_link_libraries( graphene_limit_order_status graphene_chain graphene_app )
target_include_directories( graphene_limit_order_status
                            PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include" )

if(MSVC)
  set_source_files_properties( limit_order_status_plugin.cpp PROPERTIES COMPILE_FLAGS "/bigobj" )
endif(MSVC)

install( TARGETS
   graphene_limit_order_status

   RUNTIME DESTINATION bin
   LIBRARY DESTINATION lib
   ARCHIVE DESTINATION lib
)
INSTALL( FILES ${HEADERS} DESTINATION "include/graphene/limit_order_status" )


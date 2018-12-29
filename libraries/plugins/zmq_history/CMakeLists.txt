file(GLOB HEADERS "include/graphene/zmq_history/*.hpp")
SET( ZeroMQ_LIBRARY_DIRS "/usr/lib/x86_64-linux-gnu/" )

find_library(ZeroMQ_LIBRARY
             NAMES zmq
             PATHS ${ZeroMQ_LIBRARY_DIRS}
             )

add_library( graphene_zmq_history
        zmq_history_plugin.cpp
           )

target_link_libraries( graphene_zmq_history graphene_chain graphene_app curl zmq)
target_include_directories( graphene_zmq_history PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include" )

if(MSVC)
  set_source_files_properties(zmq_history_plugin.cpp PROPERTIES COMPILE_FLAGS "/bigobj" )
endif(MSVC)

install( TARGETS
   graphene_zmq_history

   RUNTIME DESTINATION bin
   LIBRARY DESTINATION lib
   ARCHIVE DESTINATION lib
)
INSTALL( FILES ${HEADERS} DESTINATION "include/graphene/zmq_history" )


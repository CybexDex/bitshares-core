file(GLOB HEADERS "include/graphene/mongodb/*.hpp")
add_library( graphene_mongodb
        mongodb_plugin.cpp
           )
find_package(libmongocxx-static REQUIRED)
message(STATUS "Found mongocxx headers: ${LIBMONGOCXX_STATIC_INCLUDE_DIRS}")
message(STATUS "Found mongocxx libs: ${LIBMONGOCXX_STATIC_LIBRARIES}")
find_package(libbsoncxx-static REQUIRED)
message(STATUS "Found bsoncxx headers: ${LIBBSONCXX_STATIC_INCLUDE_DIRS}")
message(STATUS "Found bsoncxx libs: ${LIBBSONCXX_STATIC_LIBRARIES}")

target_link_libraries( graphene_mongodb graphene_chain graphene_app curl ${LIBMONGOCXX_STATIC_LIBRARIES} ${LIBBSONCXX_STATIC_LIBRARIES})
target_include_directories( graphene_mongodb
			    PRIVATE ${LIBMONGOCXX_STATIC_INCLUDE_DIRS} ${LIBBSONCXX_STATIC_INCLUDE_DIRS}
                            PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include" )

if(MSVC)
  set_source_files_properties(mongodb_plugin.cpp PROPERTIES COMPILE_FLAGS "/bigobj" )
endif(MSVC)

install( TARGETS
   graphene_mongodb

   RUNTIME DESTINATION bin
   LIBRARY DESTINATION lib
   ARCHIVE DESTINATION lib
)
INSTALL( FILES ${HEADERS} DESTINATION "include/graphene/mongodb" )


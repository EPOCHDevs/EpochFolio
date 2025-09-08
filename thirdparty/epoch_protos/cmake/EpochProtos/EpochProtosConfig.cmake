
####### Expanded from @PACKAGE_INIT@ by configure_package_config_file() #######
####### Any changes to this file will be overwritten by the next CMake run ####
####### The input file was EpochProtosConfig.cmake.in                            ########

get_filename_component(PACKAGE_PREFIX_DIR "${CMAKE_CURRENT_LIST_DIR}/../../../" ABSOLUTE)

macro(set_and_check _var _file)
  set(${_var} "${_file}")
  if(NOT EXISTS "${_file}")
    message(FATAL_ERROR "File or directory ${_file} referenced by variable ${_var} does not exist !")
  endif()
endmacro()

macro(check_required_components _NAME)
  foreach(comp ${${_NAME}_FIND_COMPONENTS})
    if(NOT ${_NAME}_${comp}_FOUND)
      if(${_NAME}_FIND_REQUIRED_${comp})
        set(${_NAME}_FOUND FALSE)
      endif()
    endif()
  endforeach()
endmacro()

####################################################################################

include(CMakeFindDependencyMacro)

# Find required dependencies
find_dependency(Protobuf REQUIRED)

# Include the targets file
include("${CMAKE_CURRENT_LIST_DIR}/EpochProtosTargets.cmake")

# Create the cleaner alias for consumers
if(TARGET EpochProtos::epoch_protos_cpp AND NOT TARGET epoch::protos)
    add_library(epoch::protos ALIAS EpochProtos::epoch_protos_cpp)
endif()

# Set variables for compatibility
set(EPOCHPROTOS_FOUND TRUE)
set(EPOCHPROTOS_VERSION "1.0.0")
set(EPOCHPROTOS_INCLUDE_DIRS "/usr/local/include")
set(EPOCHPROTOS_LIBRARIES epoch_protos_cpp)

check_required_components(EpochProtos)

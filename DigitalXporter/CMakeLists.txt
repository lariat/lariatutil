# We distinguish executable and non-executable .py files.
# The former are installed with execute permission.
# The latter are installed without execute permission.

# Executable python files.

LIST(APPEND exes WorkerNodeMetadataUpdate.py )

# Non-executable python files.

## LIST(APPEND nonexes somescript.py ) 

message(STATUS "Executable python modules ${exes}")
## message(STATUS "Non-executable python modules ${nonexes}")

install(PROGRAMS ${exes} DESTINATION ${product}/${version}/python)
## install(FILES ${nonexes} DESTINATION ${product}/${version}/python)
install_scripts(LIST ${exes})

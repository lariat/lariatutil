FILE(GLOB xml_files *.xml)
install(FILES ${xml_files} DESTINATION ${product}/${version}/xml/test)
install_source( EXTRAS ${xml_files} )

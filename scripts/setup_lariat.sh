# Source this file to set the basic configuration needed by LArSoft 
# and for the uBooNE-specific software that interfaces to LArSoft.

# Set up ups for LArSoft
# Sourcing this setup will add /grid/fermiapp/products/larsoft and
# /grid/fermiapp/products/common to $PRODUCTS
#

OASIS_LARSOFT_DIR="/cvmfs/oasis.opensciencegrid.org/fermilab/products/larsoft/"
FERMIAPP_LARSOFT_DIR="/grid/fermiapp/products/larsoft/"
OASIS_LARIAT_DIR="/cvmfs/oasis.opensciencegrid.org/lariat/products/"
FERMIAPP_LARIAT_DIR="/grid/fermiapp/products/lariat/"
FERMIAPP_COMMON_DIR="/grid/fermiapp/products/"
LARIAT_BLUEARC_DATA="/lariat/data/"

#if [[ -d "${FERMIAPP_COMMON_DIR}" ]]; then
#    echo "Setting up the Grid Fermiapp common UPS area...${FERMIAPP_COMMON_DIR}"
#    source ${FERMIAPP_COMMON_DIR}/setups.sh
#fi

if [[ -d "${FERMIAPP_LARSOFT_DIR}" ]]; then
    echo "Setting up the Grid Fermiapp larsoft UPS area...${FERMIAPP_LARSOFT_DIR}"
    echo /bin/bash > /dev/null
    source ${FERMIAPP_LARSOFT_DIR}/setups
    export PRODUCTS=${PRODUCTS}:/grid/fermiapp/products/common/db

elif [[ -d "${OASIS_LARSOFT_DIR}" ]]; then
    echo "Setting up the OASIS Fermilab UPS area...${OASIS_LARSOFT_DIR}"
    echo /bin/bash > /dev/null
    source ${OASIS_LARSOFT_DIR}/setups.for.cvmfs
    export PRODUCTS=${PRODUCTS}:/cvmfs/oasis.opensciencegrid.org/fermilab/products/common/db
fi

if [[ -d "${FERMIAPP_LARIAT_DIR}" ]]; then
    echo "Setting up the Grid Fermiapp lariat UPS area...${FERMIAPP_LARIAT_DIR}"
    echo /bin/bash > /dev/null
#    source ${FERMIAPP_LARIAT_DIR}/setups
    export "/grid/fermiapp/products/lariat:$PRODUCTS"

elif [[ -d "${OASIS_LARIAT_DIR}" ]]; then
    echo "Setting up the OASIS lariat UPS area...${OASIS_LARIAT_DIR}"
    echo /bin/bash > /dev/null
    source ${OASIS_LARIAT_DIR}/setups.for.cvmfs
fi

# Add uBooNE path to FW_SEARCH_PATH
#
if [[ -d "${LARIAT_BLUEARC_DATA}" ]]; then

    [[ -n $FW_SEARCH_PATH ]] && FW_SEARCH_PATH=`dropit -e -p $FW_SEARCH_PATH ${LARIAT_BLUEARC_DATA}`
    export FW_SEARCH_PATH=${LARIAT_BLUEARC_DATA}:${FW_SEARCH_PATH}

fi


# Set up the basic tools that will be needed
#
setup git
setup gitflow
setup mrb

# Define the value of MRB_PROJECT. This can be used
# to drive other set-ups. 
# We need to set this to 'larsoft' for now.

export MRB_PROJECT=larsoft

# Define environment variables that store the standard experiment name.

export JOBSUB_GROUP=lariat
export EXPERIMENT=lariat
export SAM_EXPERIMENT=lariat

# For Art workbook

export ART_WORKBOOK_OUTPUT_BASE=/lariat/data/users
export ART_WORKBOOK_WORKING_BASE=/lariat/app/users
export ART_WORKBOOK_QUAL="s2:e5:nu"

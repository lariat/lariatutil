# Source this file to set the basic configuration needed by LArSoft 
# and for the LArIAT-specific software that interfaces to LArSoft.

FERMIAPP_LARSOFT1_DIR="/grid/fermiapp/products/larsoft/"
FERMIOSG_LARSOFT1_DIR="/cvmfs/fermilab.opensciencegrid.org/products/larsoft/"

FERMIOSG_LARSOFT2_DIR="/cvmfs/larsoft.opensciencegrid.org/products/"

FERMIAPP_LARIAT_DIR="/grid/fermiapp/products/lariat/"
FERMIOSG_LARIAT_DIR="/cvmfs/lariat.opensciencegrid.org/products/"

LARIAT_BLUEARC_DATA="/lariat/data/"

# Set up ups for LArSoft
# Sourcing this setup will add larsoft and common to $PRODUCTS

for dir in $FERMIOSG_LARSOFT1_DIR $FERMIAPP_LARSOFT1_DIR;
do
  if [[ -f $dir/setup ]]; then
    echo "Setting up old larsoft UPS area... ${dir}"
    source $dir/setup
    common=`dirname $dir`/common/db
    if [[ -d $common ]]; then
      export PRODUCTS=`dropit -p $PRODUCTS common/db`:`dirname $dir`/common/db
    fi
    break
  fi
done

# Sourcing this setup will add new larsoft to $PRODUCTS

for dir in $FERMIOSG_LARSOFT2_DIR
do
  if [[ -f $dir/setup ]]; then
    echo "Setting up new larsoft UPS area... ${dir}"
    source $dir/setup
    break
  fi
done

# Set up ups for LArIAT

for dir in $FERMIOSG_LARIAT_DIR $FERMIAPP_LARIAT_DIR;
do
  if [[ -f $dir/setup ]]; then
    echo "Setting up lariat UPS area... ${dir}"
    source $dir/setup
    break
  fi
done

# Add current working directory (".") to FW_SEARCH_PATH
#
if [[ -n "${FW_SEARCH_PATH}" ]]; then
  FW_SEARCH_PATH=`dropit -e -p $FW_SEARCH_PATH .`
  export FW_SEARCH_PATH=.:${FW_SEARCH_PATH}
else
  export FW_SEARCH_PATH=.
fi

# Add LArIAT data path to FW_SEARCH_PATH
#
if [[ -d "${LARIAT_BLUEARC_DATA}" ]]; then

    if [[ -n "${FW_SEARCH_PATH}" ]]; then
      FW_SEARCH_PATH=`dropit -e -p $FW_SEARCH_PATH ${LARIAT_BLUEARC_DATA}`
      export FW_SEARCH_PATH=${LARIAT_BLUEARC_DATA}:${FW_SEARCH_PATH}
    else
      export FW_SEARCH_PATH=${LARIAT_BLUEARC_DATA}
    fi

fi

# Set up the basic tools that will be needed
#
if [ `uname` != Darwin ]; then

  # Work around git table file bugs.

  export PATH=`dropit git`
  export LD_LIBRARY_PATH=`dropit -p $LD_LIBRARY_PATH git`
  setup git
fi
setup gitflow
setup mrb

# Define the value of MRB_PROJECT. This can be used
# to drive other set-ups. 
# We need to set this to 'larsoft' for now.

export MRB_PROJECT=larsoft

# Define environment variables that store the standard experiment name.

export JOBSUB_GROUP=lariat
export EXPERIMENT=lariat     # Used by ifdhc
export SAM_EXPERIMENT=lariat

# For Art workbook

export ART_WORKBOOK_OUTPUT_BASE=/lariat/data/users
export ART_WORKBOOK_WORKING_BASE=/lariat/app/users
export ART_WORKBOOK_QUAL="s2:e5:nu"

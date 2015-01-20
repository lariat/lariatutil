#!/usr/bin/env python
#----------------------------------------------------------------------
#
# Name: project_utilities.py
#
# Purpose: A python module containing various experiment-specific
#          python utility functions.
#
# Created: 28-Oct-2013  H. Greenlee
#
#----------------------------------------------------------------------

import os

# Don't fail (on import) if samweb is not available.

try:
    import samweb_cli
except ImportError:
    pass

def get_dropbox(filename):

    # Get metadata.

    md = {}
    exp = 'lariat'
    if os.environ.has_key('SAM_EXPERIMENT'):
        exp = os.environ['SAM_EXPERIMENT']
    samweb = samweb_cli.SAMWebClient(experiment=exp)
    try:
        md = samweb.getMetadata(filenameorid=filename)
    except:
        pass

    # Extract the metadata fields that we need.
    
    file_type = ''
    group = ''
    data_tier = ''

    if md.has_key('file_type'):
        file_type = md['file_type']
    if md.has_key('group'):
        group = md['group']
    if md.has_key('data_tier'):
        data_tier = md['data_tier']

    if not file_type or not group or not data_tier:
        raise RuntimeError, 'Missing or invalid metadata for file %s.' % filename

    # Construct dropbox path.

    path = '/lariat/data/lariatsoft/dropbox/%s/%s/%s' % (file_type, group, data_tier)
    return path

# Return fcl configuration for experiment-specific sam metadata.

def get_sam_metadata(project, stage):
    result = 'services.user.FileCatalogMetadataLArIAT: {\n'
    result = result + '  FCLName: "%s"\n' % os.path.basename(stage.fclname)
    result = result + '  FCLVersion: "%s"\n' % project.release_tag
    result = result + '  ProjectName: "%s"\n' % project.name
    result = result + '  ProjectStage: "%s"\n' % stage.name
    result = result + '  ProjectVersion: "%s"\n' % project.release_tag
    result = result + '}\n'
    return result

# Function to return path to the setup_lariat.sh script

def get_setup_script_path():

    OASIS_DIR="/cvmfs/oasis.opensciencegrid.org/lariat/products/"
    FERMIAPP_DIR="/grid/fermiapp/lariat/"

    if os.path.isfile(FERMIAPP_DIR+"setup_lariat.sh"):
        setup_script = FERMIAPP_DIR+"setup_lariat.sh"
    elif os.path.isfile(OASIS_DIR+"setup_lariat.sh"):
        setup_script = OASIS_DIR+"setup_lariat.sh"
    else:
        raise RuntimeError, "Could not find setup script at "+FERMIAPP_DIR+" or "+OASIS_DIR

    return setup_script

# Construct dimension string for project, stage.

def dimensions(project, stage):

    dim = 'file_type %s' % project.file_type
    dim = dim + ' and data_tier %s' % stage.data_tier
    dim = dim + ' and ub_project.name %s' % project.name
    dim = dim + ' and ub_project.stage %s' % stage.name
    dim = dim + ' and ub_project.version %s' % project.release_tag
    dim = dim + ' and availability: anylocation'
    return dim

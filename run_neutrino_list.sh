#!/bin/bash

SNDBUILD_DIR="/afs/cern.ch/work/c/cvilela/public/SND_Oct25_neutrinoSelDev_Oct7/sw"
SNDBUILD_RELEASE="sndsw/latest-feature-second_stage_nu_filter-release"
SNDBUILD_DIR_RECO="/afs/cern.ch/work/c/cvilela/public/SND_Oct25_adapt_root_6p36/sw"
SNDBUILD_RELEASE_RECO="sndsw/latest-adapt_root_6p36-release"
SOURCE_SCRIPT="/cvmfs/sndlhc.cern.ch/SNDLHC-2025/Oct7/setUp.sh"

BASE_OUT_DIR=$1
IFS=',' read -r -a RUN_LIST <<< "${2}"
MODE=$3
CUT_SET=$4
PROCNAME=$5
GEOFILE=$6
INPUT_BASE=$7
RECO_BASE=$8

mode_list=(FILTER RECO)

RETURN=0

if [[ ! ${mode_list[@]} =~ $MODE ]]
then
    echo Mode $MODE not available. It must be one of "${mode_list[*]}"
    echo Exitting.
    exit 0 # Do not retry
fi

for input_run_dir in "${RUN_LIST[@]}"
do
    this_run_dir=`basename ${input_run_dir}`
    i_run=`echo ${this_run_dir} | tr -cd '[[:digit:]]'`
    output_run_dir=${BASE_OUT_DIR}/${this_run_dir}

    if [ "$MODE" == "FILTER" ] 
    then
	if [ "$INPUT_BASE" == "NONE" ]
	   then
	       # Check if only one digitized file exists in the input directory. Otherwise, skip.
	       #	digi_files=(${input_run_dir}/*_digCPP.root)
	       digi_files=(${input_run_dir}/*_20240126_digCPP.root)
	       echo "${digi_files[@]}"
	       if [ ! -f "${digi_files[@]}" ]
	       then
		   echo "Didn't find digitized files in ${input_run_dir}. TRYING real data sndsw_raw-*.root"
		   digi_files=(${input_run_dir}/sndsw_raw-*.root)
		   if [ ! -f "${digi_files[0]}" ]
		   then
		       echo "Didn't find real data. Continuing"
		       continue
		   else
		       REAL_DATA=1
		   fi
	       else 
		   REAL_DATA=0
	       fi

	       if [ "$REAL_DATA" -ne "1" ]
	       then
		   echo "NOT REAL DATA"
		   # Input file name
		   input_file=${digi_files[0]}
	       else 
		   echo "REAL DATA"
		   input_file=${input_run_dir}
		   input_file+='/sndsw_raw-*.root'
	       fi
	else
	    input_file=${output_run_dir}"/"${INPUT_BASE}_${i_run}.root
	    echo "USER DEFINED INPUT"
	fi

	echo "${input_file}"

	if [ -f "${output_run_dir}/snd_event_filter_${PROCNAME}_${i_run}.root" ]
	    then
	    echo "File ${output_run_dir}/snd_event_filter_${PROCNAME}_${i_run}.root exists. Skipping."
	    continue
	fi

    	# Set up SND environment
	if [ -z ${SNDSW_ROOT+x} ]
	then
	    echo "Setting up SNDSW"
    	    source $SOURCE_SCRIPT
    	    eval `alienv load -w $SNDBUILD_DIR --no-refresh $SNDBUILD_RELEASE`
	    eval `alienv -w $SNDBUILD_DIR list`
	    export EOSSHIP=root://eosuser.cern.ch/
	fi

	# Run first stage filter
	# CHECK IF RECO_BASE EXISTS
	if [[ $RECO_BASE == "NONE" ]]
	then
	    echo sndEventFilter --input "${input_file}" --output snd_event_filter_${PROCNAME}_${i_run}.root --geofile ${GEOFILE} --pipeline ${SNDSW_ROOT}/${CUT_SET}
	    sndEventFilter --input "${input_file}" --output snd_event_filter_${PROCNAME}_${i_run}.root --geofile ${GEOFILE} --pipeline ${SNDSW_ROOT}/${CUT_SET} &> snd_event_filter_${PROCNAME}_${i_run}.log
	else
	    echo sndEventFilter --input "${input_file}" --output snd_event_filter_${PROCNAME}_${i_run}.root --geofile ${GEOFILE} --pipeline ${SNDSW_ROOT}/${CUT_SET} --recofile ${output_run_dir}/${RECO_BASE}_${i_run}_${i_run}_muonReco.root
	    sndEventFilter --input "${input_file}" --output snd_event_filter_${PROCNAME}_${i_run}.root --geofile ${GEOFILE} --pipeline ${SNDSW_ROOT}/${CUT_SET} --recofile ${output_run_dir}/${RECO_BASE}_${i_run}_${i_run}_muonReco.root &> snd_event_filter_${PROCNAME}_${i_run}.log
	fi

	if [ $? != "0" ]
	then
	    RETURN=1
	fi
	
	# Copy output
   	eos mkdir -p ${output_run_dir}/
	xrdcp -f snd_event_filter_${PROCNAME}_${i_run}.root ${output_run_dir}/snd_event_filter_${PROCNAME}_${i_run}.root
	xrdcp -f snd_event_filter_${PROCNAME}_${i_run}.log ${output_run_dir}/snd_event_filter_${PROCNAME}_${i_run}.log

	rm -rf snd_event_filter_${PROCNAME}_${i_run}.root snd_event_filter_${PROCNAME}_${i_run}.log

    elif [ "$MODE" == "RECO" ] 
    then

	if [ -f "${output_run_dir}/${RECO_BASE}_${i_run}__muonReco.root" ]
	    then
	    echo "File ${output_run_dir}/${RECO_BASE}_${i_run}__muonReco.root exists. Skipping."
	    continue
	fi

	if [ ! -f "${output_run_dir}/${INPUT_BASE}_${i_run}.root" ]
	    then
	    echo "File ${output_run_dir}/${INPUT_BASE}_${i_run}.root does not exist. Skipping."
	    continue
	fi

    	# Set up SND environment
	if [ -z ${SNDSW_ROOT+x} ]
	then
	    echo "Setting up SNDSW"
	    source $SOURCE_SCRIPT
    	    eval `alienv load -w $SNDBUILD_DIR_RECO --no-refresh $SNDBUILD_RELEASE_RECO`
	    export EOSSHIP=root://eosuser.cern.ch/
	fi

	echo python $SNDSW_ROOT/shipLHC/run_muonRecoSND.py -f ${output_run_dir}/${INPUT_BASE}_${i_run}.root -g ${GEOFILE} -c passing_mu_DS -sc 1 -s ./ -hf linearSlopeIntercept -o
	python $SNDSW_ROOT/shipLHC/run_muonRecoSND.py -f ${output_run_dir}/${INPUT_BASE}_${i_run}.root -g ${GEOFILE} -c passing_mu_DS -sc 1 -s ./ -hf linearSlopeIntercept -o &> snd_muon_reco_${i_run}.log
	
	if [ $? != "0" ]
	then
	    RETURN=1
	fi

    	xrdcp -f ./${INPUT_BASE}_${i_run}_${i_run}_muonReco.root ${output_run_dir}/${INPUT_BASE}_${i_run}_${i_run}_muonReco.root
	xrdcp -f ./snd_muon_reco_${i_run}.log ${output_run_dir}/snd_event_filter_${PROCNAME}_${i_run}.log
	rm ./${INPUT_BASE}_${i_run}_${i_run}_muonReco.root ./snd_event_filter_${PROCNAME}_${i_run}.log
    fi
done

exit $RETURN

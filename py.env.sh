#!/bin/sh

VROOT=penv

conda_environment() {
    #echo "creating virtual environment $VROOT"
    conda create --name $VROOT --yes
}

conda_activate() {
    source activate $VROOT
}

pip_environment() {
    python3 -m venv $VROOT
}

pip_activate() {
    . ${VROOT}/bin/activate
}

pip_install_requirements() {
    pip install --upgrade pip
    pip install wheel
    pip install --editable .
}

if type conda >/dev/null 2>&1 ; then
    #echo "well done, setting up anaconda env"
    source activate $VROOT > /dev/null 2>&1
    if [ ! $? -eq 0 ]; then
        conda_environment
        source activate $VROOT
    fi
elif [ ! -f ${VROOT}/bin/activate ] ; then
    #echo "creating a standard virtual env"
    pip_environment
    pip_activate
    pip_install_requirements
else
    #echo "activating a standard virtual env"
    pip_activate
fi

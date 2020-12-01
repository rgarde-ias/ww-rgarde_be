"""AWS CDK application.

See https://docs.aws.amazon.com/cdk/ for details.

"""

from ias_pmi_cdk_common import PMIApp

from stacks import MainStack


APP_NAME = 'etl-pm-pipeline-be'


# create CDK application
app = PMIApp(APP_NAME)

# add stacks
MainStack(app, app, 'main')

# synthesize application assembly
app.synth()

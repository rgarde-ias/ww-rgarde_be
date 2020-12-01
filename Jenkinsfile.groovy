final approverGroup = 'integralads*Weed Wackers'
final chefApiKeyCredsId = 'airflow-chef-client'

final settings = [
    pipeline_generator: global_pipeline_generator,
    additional_credentials: ['airflow-gd-deploy']
]

createTagOnMergeTo(version_file: 'version.properties', branch: 'master')

if (isPRBuild()) {
    deployStage(settings, ias_env: 'dev', approvers: 'automatic') {
        withCredentials([file(credentialsId: chefApiKeyCredsId, variable: 'CHEF_API_KEY')]) {
            runToolChainsSh(settings, 'invoke -e deploy dev')
        }
    }
}

if (isTagBuild()) {
    deployStage(settings, ias_env: 'staging', approvers: approverGroup) {
        withCredentials([file(credentialsId: chefApiKeyCredsId, variable: 'CHEF_API_KEY')]) {
            runToolChainsSh(settings, 'invoke -e deploy staging')
        }
    }
    deployStage(settings, ias_env: 'prod', approvers: approverGroup) {
        withCredentials([file(credentialsId: chefApiKeyCredsId, variable: 'CHEF_API_KEY')]) {
            runToolChainsSh(settings, 'invoke -e deploy prod')
        }
    }
}

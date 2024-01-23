package main

deny[msg] {
    input.version < 2
    msg = "Use version 2 or higher"
}

deny[msg] {
    input.jobs[_].machine.docker_layer_caching == true
    msg = "Don't use DLC"
}

deny[msg] {
    input.jobs[_].steps[_].setup_remote_docker.docker_layer_caching == true
    msg = "Don't use DLC"
}

deny[msg] {
    not input.workflows
    msg = "Use workflows"
}

deny[msg] {
    input.jobs[_].circleci_ip_ranges == true
    msg = "Don't use IP ranges"
}

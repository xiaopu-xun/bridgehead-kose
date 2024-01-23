package main

test_deny_under_20 {
    deny["Use version 2 or higher"] with input as {"version": 1}
}

test_allow_version_21 {
    not deny["Use version 2 or higher"] with input as {"version": 2.1}
}

test_deny_dlc_docker {
    deny["Don't use DLC"] with input as 
    {
        "version": 2.1,
        "jobs": {
            "build": {
                "steps": [
                    "checkout",
                    {
                        "setup_remote_docker": {
                            "docker_layer_caching": true,
                        },
                    },
                    {
                        "run": {
                            "docker build .",
                        }
                    }
                ],
            }
        }
    }
}

test_allow_dlc_docker_not_in_use {
    not deny["Don't use DLC"] with input as 
    {
        "version": 2.1,
        "jobs": {
            "build": {
                "steps": [
                    "checkout",
                    {
                        "run": {
                            "foo bar",
                        }
                    }
                ],
            }
        }
    }
}


test_allow_dlc_docker_not_in_use {
    not deny["Don't use DLC"] with input as 
    {
        "version": 2.1,
        "jobs": {
            "hogefuga": {
                "steps": [
                    "checkout",
                    {
                        "setup_remote_docker": {
                            "docker_layer_caching": false,
                        },
                    },
                    {
                        "run": {
                            "foo bar",
                        }
                    }
                ],
            }
        }
    }
}


test_deny_dlc_machine {
    deny["Don't use DLC"] with input as 
    {
        "version": 2,
        "jobs": {
            "make": {
                "machine": {
                    "docker_layer_caching": true
                }
            }
        }
    }
}

test_allow_using_workflows {
    not deny["Use workflows"] with input as 
    {
        "version": 2.1,
        "workflows": {
            "version": 2
        }
    }
}


test_deny_not_in_use_workflows {
    deny["Use workflows"] with input as 
    {
        "version": 2.1,
        "workflow": {
            "version": 2
        }
    }
}

test_deny_ip_ranges_docker {
    deny["Don't use IP ranges"] with input as 
    {
        "version": 2.1,
        "jobs": {
            "build": {
                "circleci_ip_ranges": true,
                "steps": [
                    "checkout",
                    {
                        "setup_remote_docker": {
                            "docker_layer_caching": true,
                        },
                    },
                    {
                        "run": {
                            "docker build .",
                        }
                    }
                ],
            }
        }
    }
}

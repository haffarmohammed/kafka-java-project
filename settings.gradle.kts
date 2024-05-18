rootProject.name = "kafka-java-project"
include("kafka-basics:kafka-basics")
findProject(":kafka-basics:kafka-basics")?.name = "kafka-basics"

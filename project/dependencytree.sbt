resolvers += Resolver.url(
  "eportal artifactory",
  url(s"""${sys.env.getOrElse("ARTIFACTORY_HOST", "https://eportal-artifacts.fpprod.corp/artifactory")}/${sys.env
    .getOrElse("ARTIFACTORY_REPO_SBT", "eportal-sbt")}""")
)(Resolver.defaultIvyPatterns)

addSbtPlugin("com.leonteq.eportal"   % "eportal-sbt-settings" % "1.4.7")

addDependencyTreePlugin
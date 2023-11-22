import scala.util.Try

ThisBuild / credentials += {
  val envCred = for {
    pwd <- sys.env.get("PASSWORD")
    user <- sys.env.get("USERNAME")
  } yield Credentials("Artifactory Realm", "repository.rnd.amadeus.net", user, pwd)

  envCred
    .orElse {
      val credFile = Try(Path.userHome / ".artifactory" / "credentials").toOption
      credFile.map(Credentials(_))
    }
    .getOrElse {
      Credentials("Artifactory Realm", "repository.rnd.amadeus.net", "anonymous", "")
    }
}

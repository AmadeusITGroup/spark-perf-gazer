// This is to use authentication to connect to Amadeus artifactory when fetching sbt plugins
import scala.util.Try

credentials += {
  val envCred = for {
    pwd <- sys.env.get("PASSWORD")
    user <- sys.env.get("USERNAME")
  } yield Credentials("Artifactory Realm", "repository.rnd.amadeus.net", user, pwd)

  envCred.orElse {
    val credFile = Try(Path.userHome / ".artifactory" / "credentials").toOption
    credFile.map(Credentials(_))
  }.getOrElse {
    Credentials("Artifactory Realm", "repository.rnd.amadeus.net", "anonymous", "")
  }
}

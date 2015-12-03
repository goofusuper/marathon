package mesosphere.marathon.api.v2.json

import java.lang.{ Integer => JInt, Double => JDouble }
import java.net.{URLConnection, HttpURLConnection, URL}

import com.wix.accord.{RuleViolation, Failure, Success, Validator}
import com.wix.accord.dsl._
import mesosphere.marathon.api.v2.Validation._

import mesosphere.marathon.api.v2.json.V2AppDefinition.VersionInfo
import mesosphere.marathon.api.validation.FieldConstraints._
import mesosphere.marathon.health.HealthCheck
import mesosphere.marathon.Protos.Constraint
import mesosphere.marathon.state.{
  AppDefinition,
  Container,
  PathId,
  UpgradeStrategy,
  Timestamp
}

import scala.collection.immutable.Seq
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

case class V2AppUpdate(

    id: Option[PathId] = None,

    cmd: Option[String] = None,

    args: Option[Seq[String]] = None,

    user: Option[String] = None,

    env: Option[Map[String, String]] = None,

    instances: Option[JInt] = None,

    cpus: Option[JDouble] = None,

    mem: Option[JDouble] = None,

    disk: Option[JDouble] = None,

    executor: Option[String] = None,

    constraints: Option[Set[Constraint]] = None,

    uris: Option[Seq[String]] = None,

    storeUrls: Option[Seq[String]] = None,

    @FieldPortsArray ports: Option[Seq[JInt]] = None,

    requirePorts: Option[Boolean] = None,

    backoff: Option[FiniteDuration] = None,

    backoffFactor: Option[JDouble] = None,

    maxLaunchDelay: Option[FiniteDuration] = None,

    container: Option[Container] = None,

    healthChecks: Option[Set[HealthCheck]] = None,

    dependencies: Option[Set[PathId]] = None,

    upgradeStrategy: Option[UpgradeStrategy] = None,

    labels: Option[Map[String, String]] = None,

    acceptedResourceRoles: Option[Set[String]] = None,

    version: Option[Timestamp] = None) {

  require(version.isEmpty || onlyVersionOrIdSet, "The 'version' field may only be combined with the 'id' field.")

  protected[api] def onlyVersionOrIdSet: Boolean = productIterator forall {
    case x @ Some(_) => x == version || x == id
    case _           => true
  }

  /**
    * Returns the supplied [[mesosphere.marathon.state.AppDefinition]] after
    * updating its members with respect to this update request.
    */
  def apply(app: AppDefinition): AppDefinition =
    apply(V2AppDefinition(app)).toAppDefinition

  /**
    * Returns the supplied [[mesosphere.marathon.api.v2.json.V2AppDefinition]]
    * after updating its members with respect to this update request.
    */
  def apply(app: V2AppDefinition): V2AppDefinition = app.copy(
    id = app.id,
    cmd = cmd.orElse(app.cmd),
    args = args.orElse(app.args),
    user = user.orElse(app.user),
    env = env.getOrElse(app.env),
    instances = instances.getOrElse(app.instances),
    cpus = cpus.getOrElse(app.cpus),
    mem = mem.getOrElse(app.mem),
    disk = disk.getOrElse(app.disk),
    executor = executor.getOrElse(app.executor),
    constraints = constraints.getOrElse(app.constraints),
    uris = uris.getOrElse(app.uris),
    storeUrls = storeUrls.getOrElse(app.storeUrls),
    ports = ports.getOrElse(app.ports),
    requirePorts = requirePorts.getOrElse(app.requirePorts),
    backoff = backoff.getOrElse(app.backoff),
    backoffFactor = backoffFactor.getOrElse(app.backoffFactor),
    maxLaunchDelay = maxLaunchDelay.getOrElse(app.maxLaunchDelay),
    container = container.filterNot(_ == Container.Empty).orElse(app.container),
    healthChecks = healthChecks.getOrElse(app.healthChecks),
    dependencies = dependencies.map(_.map(_.canonicalPath(app.id))).getOrElse(app.dependencies),
    upgradeStrategy = upgradeStrategy.getOrElse(app.upgradeStrategy),
    labels = labels.getOrElse(app.labels),
    acceptedResourceRoles = acceptedResourceRoles.orElse(app.acceptedResourceRoles),
    version = version.getOrElse(app.version)
  )

  def withCanonizedIds(base: PathId = PathId.empty): V2AppUpdate = copy(
    id = id.map(_.canonicalPath(base)),
    dependencies = dependencies.map(_.map(_.canonicalPath(base)))
  )
}

object V2AppUpdate {
  implicit val appUpdateValidator = validator[V2AppUpdate] { appUp =>
    appUp.id is valid
    appUp.dependencies is valid
    appUp.upgradeStrategy is valid
    appUp.storeUrls is optional(every(urlsCanBeResolvedValidator))
  }

  def urlsCanBeResolvedValidator: Validator[String] = {
    new Validator[String] {
      def apply(url: String) = {
        Try {
          new URL(url).openConnection() match {
            case http: HttpURLConnection =>
              http.setRequestMethod("HEAD")
              if(http.getResponseCode == HttpURLConnection.HTTP_OK) Success
              else Failure(Set(RuleViolation(url, "url could not be resolved", None)))
            case other: URLConnection =>
              other.getInputStream
              Success //if we come here, we could read the stream
          }
        }.getOrElse(
          Failure(Set(RuleViolation(url, "url could not be resolved", None)))
        )
      }
    }
  }
}
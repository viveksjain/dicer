package com.databricks.conf

/**
 * The main Databricks configuration library. All configuration properties are loaded during
 * construction time, so that errors can be detected immediately. This does mean that creating
 * a Conf can be expensive, though, so the objects should be reused as much as possible.
 * Conf objects are immutable by nature.
 *
 * === Section 1 - Specifying Configuration ===
 * Configuration is specified straightforwardly by default:
 *   val myConfig = configure[String]("databricks.username", "bob")
 * The type can be left out if it is obvious (the default "bob" here makes String extraneous,
 * for example).
 *
 * We use Jackson to deserialize configuration. This means we can inherently deserialize
 * all types available via [[com.databricks.rpc.JsonSerializable]]:
 * Strings, primitives, arrays, maps, etc., up to whole objects specified in JSON. As our
 * configuration file format is JSON-based, storing objects is actually very simple. See Section 3.
 *
 * === Section 2 - Creating and Combining Configuration Objects ===
 * There are two types of classes that may extend DbConf. The first is a Service Configuration,
 * such as ManagerConf, which requires a concrete Conf file to run a real program. The second is
 * a Configuration Trait, such as AwsConf, which simply specifies a set of configuration parameters
 * related to some helper class or functionality.
 *
 * The advantage of this model is that Service Configuration classes may arbitrarily mix-in
 * Configuration Traits as they are needed, rather than having several concrete Configuration
 * objects. For example, as ManagerConf uses AWS, it is defined like this:
 *   class ManagerConf extends DbConfImpl with AwsConf
 * and thus ManagerConf is-a AwsConf, and automatically includes all of the parameters in AwsConf.
 *
 * === Section 3 - Configuration Variables ===
 * Conf is loaded from the environment variable `DB_CONF` as a JSON string.
 * So for a configuration option defined like:
 *   val myFlag = configure[Boolean]("databricks.myFlag")
 * you can supply the environment variable `DB_CONF` in kubernetes like this:
 *   env:
 *   - name: DB_CONF
 *   - value: '{"databricks.myFlag": true}'
 *
 * See DbConfImpl and ProjectConf for more details on how we load actual configuration.
 */
trait DbConf {

  /**
   * Returns the given property as a type T (assuming T is a simple type that can be deserialized
   * from JSON) from the DB_CONF configuration, or if it is not specified then returns the provided
   * default value. Uses [[DefaultParser]] which handles list deserialization.
   */
  protected def configure[T: Manifest](propertyName: String, defaultValue: T): T = {
    configure(propertyName, defaultValue, new DefaultParser[T]())
  }

  /**
   * Returns the given property as a type T using the given parser from the DB_CONF configuration,
   * or if it is not specified then returns the provided default value.
   */
  protected def configure[T: Manifest](
      propertyName: String,
      defaultValue: T,
      parser: ConfigParser[T]): T = {
    doConfigure(propertyName, parser).getOrElse(defaultValue)
  }

  /**
   * Returns the given property as a type T if it exists in the DB_CONF configuration, or None if
   * not. If the property exists but does not parse to the expected type, an exception will be
   * thrown.
   */
  protected def doConfigure[T: Manifest](propertyName: String, parser: ConfigParser[T]): Option[T]
}

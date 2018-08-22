import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.OffsetDateTime;
import java.util.List;

public class Database {
  static class Details {
    public final String host;
    public final String port;
    public final String dbName;
    public final String user;
    public final String password;
    public final String tunnelPort;
    public final boolean ssl;

    @JsonCreator
    public Details(
        @JsonProperty("host") String host,
        @JsonProperty("port") String port,
        @JsonProperty("dbname") String dbName,
        @JsonProperty("user") String user,
        @JsonProperty("password") String password,
        @JsonProperty("tunnel-port") String tunnelPort,
        @JsonProperty("ssl") boolean ssl
    ) {
      this.host = host;
      this.port = port;
      this.dbName = dbName;
      this.user = user;
      this.password = password;
      this.tunnelPort = tunnelPort;
      this.ssl = ssl;
    }
  }

  public final String description;
  public final List<String> features;
  public final String cacheFieldValuesSchedule;
  public final String timezone;
  public final String metaDataSyncSchedule;
  public final String name;
  public final String caveats;
  public final boolean isFullSync;
  public final OffsetDateTime updateAt;
  public final String nativePermissions;
  public final Details details;
  public final boolean isSample;
  public final int id;
  public final boolean isOnDemand;
  public final String engine;
  public final OffsetDateTime createAt;
  public final String pointsOfInterest;


  @JsonCreator
  Database(
      @JsonProperty("description") String description,
      @JsonProperty("features") List<String> features,
      @JsonProperty("cache_field_values_schedule") String cacheFieldValuesSchedule,
      @JsonProperty("timezone") String timezone,
      @JsonProperty("metadata_sync_schedule") String metaDataSyncSchedule,
      @JsonProperty("name") String name,
      @JsonProperty("caveats") String caveats,
      @JsonProperty("is_full_sync") boolean isFullSync,
      @JsonProperty("updated_at") String updatedAt,
      @JsonProperty("native_permissions") String nativePermissions,
      @JsonProperty("details") Details details,
      @JsonProperty("is_sample") boolean isSample,
      @JsonProperty("id") int id,
      @JsonProperty("is_on_demand") boolean isOnDemand,
      @JsonProperty("engine") String engine,
      @JsonProperty("created_at") String createAt,
      @JsonProperty("points_of_interest") String pointsOfInterest
  ) {
    this.description = description;
    this.features = features;
    this.cacheFieldValuesSchedule = cacheFieldValuesSchedule;
    this.timezone = timezone;
    this.metaDataSyncSchedule = metaDataSyncSchedule;
    this.name = name;
    this.caveats = caveats;
    this.isFullSync = isFullSync;
    this.updateAt = OffsetDateTime.parse(updatedAt);
    this.nativePermissions = nativePermissions;
    this.details = details;
    this.isSample = isSample;
    this.id = id;
    this.isOnDemand = isOnDemand;
    this.engine = engine;
    this.createAt = OffsetDateTime.parse(createAt);
    this.pointsOfInterest = pointsOfInterest;
  }
}

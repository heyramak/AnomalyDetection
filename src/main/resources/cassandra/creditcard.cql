CREATE KEYSPACE IF NOT EXISTS intrusionDetection
    WITH replication = {
        'class': 'SimpleStrategy', 'replication_factor': 1
        };

USE intrusionDetection;

CREATE TABLE IF NOT EXISTS normal
(
    id                          UUID,
    duration                    double,
    protocol_type               text,
    service                     text,
    flag                        text,
    src_bytes                   double,
    dst_bytes                   double,
    land                        double,
    wrong_fragment              double,
    urgent                      double,
    hot                         double,
    num_failed_logins           double,
    logged_in                   double,
    num_compromised             double,
    root_shell                  double,
    su_attempted                double,
    num_root                    double,
    num_file_creations          double,
    num_shells                  double,
    num_access_files            double,
    num_outbound_cmds           double,
    is_host_login               double,
    is_guest_login              double,
    count                       double,
    srv_count                   double,
    serror_rate                 double,
    srv_serror_rate             double,
    rerror_rate                 double,
    srv_rerror_rate             double,
    same_srv_rate               double,
    diff_srv_rate               double,
    srv_diff_host_rate          double,
    dst_host_count              double,
    dst_host_srv_count          double,
    dst_host_same_srv_rate      double,
    dst_host_diff_srv_rate      double,
    dst_host_same_src_port_rate double,
    dst_host_srv_diff_host_rate double,
    dst_host_serror_rate        double,
    dst_host_srv_serror_rate    double,
    dst_host_rerror_rate        double,
    dst_host_srv_rerror_rate    double,
    xAttack                     text
    PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS anomaly
(
    id                          UUID,
    duration                    double,
    protocol_type               text,
    service                     text,
    flag                        text,
    src_bytes                   double,
    dst_bytes                   double,
    land                        double,
    wrong_fragment              double,
    urgent                      double,
    hot                         double,
    num_failed_logins           double,
    logged_in                   double,
    num_compromised             double,
    root_shell                  double,
    su_attempted                double,
    num_root                    double,
    num_file_creations          double,
    num_shells                  double,
    num_access_files            double,
    num_outbound_cmds           double,
    is_host_login               double,
    is_guest_login              double,
    count                       double,
    srv_count                   double,
    serror_rate                 double,
    srv_serror_rate             double,
    rerror_rate                 double,
    srv_rerror_rate             double,
    same_srv_rate               double,
    diff_srv_rate               double,
    srv_diff_host_rate          double,
    dst_host_count              double,
    dst_host_srv_count          double,
    dst_host_same_srv_rate      double,
    dst_host_diff_srv_rate      double,
    dst_host_same_src_port_rate double,
    dst_host_srv_diff_host_rate double,
    dst_host_serror_rate        double,
    dst_host_srv_serror_rate    double,
    dst_host_rerror_rate        double,
    dst_host_srv_rerror_rate    double,
    xAttack                     text,
    PRIMARY KEY (id)
);



CREATE TABLE IF NOT EXISTS kafka_offset
(
    partition int,
    offset    bigint,
    PRIMARY KEY (partition)
);




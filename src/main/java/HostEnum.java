public enum HostEnum {
    YANDEX("yandex.ru", "text"),
    GOOGLE("www.google.ru", "q");

    private String host;
    private String queryParamName;

    HostEnum(String host, String queryParamName) {
        this.host = host;
        this.queryParamName = queryParamName;
    }

    public String getHost() {
        return host;
    }

    public String getQueryParamName() {
        return queryParamName;
    }

    public static HostEnum getByHost(String host) {
        for(HostEnum hostEnum : HostEnum.values()){
            if (hostEnum.host.equals(host)) {
                return hostEnum;
            }
        }
        return null;
    }
}

package graphql.example;

public class Case {
    private String description;
    private String userId;
    private User user;

    public Case() {}

    public Case(String description, String userId) {
        this.description = description;
        this.userId = userId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    @Override
    public String toString() {
        return "Case("+description+" userId="+userId+")";
    }
}

import java.util.List;

public class Review {
    private String text;
    private String link;
    private int rating;
    private int sentiment;
    private int index; //index of review in job list
    private List<String> entities;
    private boolean isDone;

    public Review(String text, String link, int rating, int index){
        this.text = text;
        this.link = link;
        this.rating = rating;
        this.index = index;
        this.isDone = false;
    }

    public String getText() {
        return text;
    }

    public String getLink() {
        return link;
    }

    public int getRating() {
        return rating;
    }

    public int getSentiment() {
        return sentiment;
    }

    public List<String> getEntities() {
        return entities;
    }

    public void setResult(List<String> entities, int sentiment) {
        this.entities = entities;
        this.sentiment = sentiment;
        this.isDone = true;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public boolean isDone() {
        return isDone;
    }

}

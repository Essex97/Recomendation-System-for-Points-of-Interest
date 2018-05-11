package distributed;

import java.io.Serializable;

public class POIS implements Serializable
{
    private String id;
    private double latitude;
    private double longtitude;
    private String photosLink;
    private String Category;
    private String name;

    public POIS(String id, double latitude, double longtitude, String photosLink, String category, String name)
    {
        this.id = id;
        this.latitude = latitude;
        this.longtitude = longtitude;
        this.photosLink = photosLink;
        Category = category;
        this.name = name;
    }

    public POIS()
    {

    }

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public double getLatitude()
    {
        return latitude;
    }

    public void setLatitude(double latitude)
    {
        this.latitude = latitude;
    }

    public double getLongtitude()
    {
        return longtitude;
    }

    public void setLongtitude(double longtitude)
    {
        this.longtitude = longtitude;
    }

    public String getPhotosLink()
    {
        return photosLink;
    }

    public void setPhotosLink(String photosLink)
    {
        this.photosLink = photosLink;
    }

    public String getCategory()
    {
        return Category;
    }

    public void setCategory(String category)
    {
        Category = category;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    @Override
    public String toString()
    {
        return "POIS{" +
                "id='" + id + '\'' +
                ", latitude=" + latitude +
                ", longtitude=" + longtitude +
                ", photosLink='" + photosLink + '\'' +
                ", Category='" + Category + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}

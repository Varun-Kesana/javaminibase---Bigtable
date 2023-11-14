package BigT;

public interface IStream {
    Map getNext() throws Exception;
    void closeStream() throws Exception;

}

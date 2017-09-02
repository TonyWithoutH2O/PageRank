public class Driver {
    //args[0]: dir of transitionMatrix
    //args[1]: dir of PageRankMatrix
    //args[2]: dir of multiplyResult
    //args[3]: number of count for convergence
    public static void main(String[] args) throws Exception {

        PageRankMultiply pageRankMultiply = new PageRankMultiply();
        PageRankAdd pagerankAdd = new PageRankAdd();
        String tsMatrix = args[0];
        String prMatrix = args[1];
        String UnitMultiply = args[2];
        int count = Integer.parseInt(args[3]);
        String beta = args[4];

        for(int i = 0; i < count; i++) {
            String[] args1 = {tsMatrix, prMatrix + i, UnitMultiply + i};
            pageRankMultiply.main(args1);
            String[] args2 = {beta, UnitMultiply + i, prMatrix + i, prMatrix + (i+1)};
            pagerankAdd.main(args2);
        }
    }
}

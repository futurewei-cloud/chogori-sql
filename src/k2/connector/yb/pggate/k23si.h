#pragma once

namespace k2gate {

class K23SITxnHandle {
   public:
    void read();
    void write();
    void query();
    void end();
};

class K23SIGate {
   public:
    K23SIGate();
    void beginTxn();

};  // class K23SIGate

}  // namespace k2gate

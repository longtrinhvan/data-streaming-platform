import React from "react";
import "../styles/Header.css";

const Header = () => {
  return (
    <header className="header">
      <div className="logo" onClick={() => window.location.hash = "/"}>
        <h1 className="blog-title">Blog</h1>
      </div>
      <nav className="menu">
        <ul>
          <li className="dropdown">
            <a href="#menu" className="menu-icon">☰</a>
            <ul className="dropdown-content">
              <li><a href="#about">About</a></li>
              <li><a href="#projects">Projects</a></li>
              <li><a href="#contact">Contact</a></li>
            </ul>
          </li>
        </ul>
      </nav>
    </header>
  );
};

export default Header;
